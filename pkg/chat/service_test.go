package chat

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/protocol/logger"
)

// TestWsConnSendPool_BoundedConcurrency verifies that the per-wsConn
// dispatcher caps concurrent in-flight jobs at wsConnSendConcurrency.
// If this regressed (e.g., the semaphore was removed or sized wrong),
// peak concurrent count would exceed 64 and the WS could spawn an
// unbounded number of long-running SendAll goroutines.
func TestWsConnSendPool_BoundedConcurrency(t *testing.T) {
	wc := newWsConn(nil) // tests don't touch writeJSON; nil conn is fine
	go wc.runSendDispatcher(logger.GetLogger())
	defer func() {
		close(wc.sendDone)
		<-wc.sendDispatcherDone
		wc.sendWG.Wait()
	}()

	const jobs = 200
	var (
		inFlight int64
		peak     int64
		done     sync.WaitGroup
	)
	done.Add(jobs)
	release := make(chan struct{})

	for i := 0; i < jobs; i++ {
		job := func() {
			n := atomic.AddInt64(&inFlight, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
					break
				}
			}
			<-release
			atomic.AddInt64(&inFlight, -1)
			done.Done()
		}
		select {
		case wc.sendQueue <- job:
		case <-time.After(2 * time.Second):
			t.Fatalf("enqueue %d timed out", i)
		}
	}

	// Give the dispatcher a moment to settle into peak concurrency.
	time.Sleep(50 * time.Millisecond)

	gotPeak := atomic.LoadInt64(&peak)
	if gotPeak > wsConnSendConcurrency {
		t.Fatalf("peak concurrency=%d exceeds cap=%d", gotPeak, wsConnSendConcurrency)
	}
	// Sanity: we should actually have HIT the cap with 200 jobs.
	if gotPeak < wsConnSendConcurrency {
		t.Errorf("peak concurrency=%d; expected to saturate at %d with %d jobs",
			gotPeak, wsConnSendConcurrency, jobs)
	}

	close(release)
	done.Wait()
}

// TestWsConnSendPool_QueueOverflow verifies that once the pool is
// saturated (concurrency slots + queue buffer full), additional
// non-blocking enqueues are rejected. The kindSend handler uses
// select-default on the same channel, so this is the exact mechanism
// that lets the WS reader fast-fail instead of blocking.
func TestWsConnSendPool_QueueOverflow(t *testing.T) {
	wc := newWsConn(nil)
	go wc.runSendDispatcher(logger.GetLogger())
	defer func() {
		close(wc.sendDone)
		<-wc.sendDispatcherDone
		wc.sendWG.Wait()
	}()

	release := make(chan struct{})
	slowJob := func() { <-release }

	// Fill concurrency slots + queue buffer. Some non-determinism in
	// dispatcher scheduling means we can't claim an EXACT capacity to
	// the byte — but we know it's >= wsConnSendConcurrency + wsConnSendQueueDepth.
	const minCapacity = wsConnSendConcurrency + wsConnSendQueueDepth
	accepted := 0
	for i := 0; i < minCapacity; i++ {
		select {
		case wc.sendQueue <- slowJob:
			accepted++
		case <-time.After(1 * time.Second):
			t.Fatalf("enqueue %d timed out (only %d accepted)", i, accepted)
		}
	}
	if accepted != minCapacity {
		t.Fatalf("accepted=%d; expected %d before any rejection", accepted, minCapacity)
	}

	// Push more until we hit the rejection branch. Try generously — the
	// dispatcher might still be picking jobs off the queue, so the first
	// few attempts could still succeed.
	rejected := false
	for i := 0; i < 50; i++ {
		select {
		case wc.sendQueue <- slowJob:
			accepted++
		default:
			rejected = true
		}
		if rejected {
			break
		}
	}
	if !rejected {
		t.Fatalf("expected at least one non-blocking enqueue to be rejected after %d accepted", accepted)
	}

	close(release)
}

// TestWsConnSendPool_ShutdownDrainsInflight verifies that closing
// sendDone unblocks the dispatcher AND that sendWG tracks in-flight
// workers so serveWS can wait them out cleanly. The serveWS defer
// does exactly this sequence; a regression here would either leak
// goroutines or hang WS teardown.
func TestWsConnSendPool_ShutdownDrainsInflight(t *testing.T) {
	wc := newWsConn(nil)
	go wc.runSendDispatcher(logger.GetLogger())

	var ran int64
	const n = 10
	var done sync.WaitGroup
	done.Add(n)
	for i := 0; i < n; i++ {
		wc.sendQueue <- func() {
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt64(&ran, 1)
			done.Done()
		}
	}

	done.Wait()
	close(wc.sendDone)
	<-wc.sendDispatcherDone

	waitDone := make(chan struct{})
	go func() {
		wc.sendWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("sendWG.Wait did not return after shutdown")
	}
	if got := atomic.LoadInt64(&ran); got != n {
		t.Errorf("ran=%d; want %d", got, n)
	}
}

// TestWsConnSendPool_SaturatedDoesNotBlockShutdown verifies that even
// when all 64 concurrency slots are occupied AND the dispatcher is
// blocked trying to acquire another slot, close(sendDone) unblocks
// the dispatcher's acquire (the cancel-able select) so shutdown
// doesn't hang waiting for slow jobs to finish first.
func TestWsConnSendPool_SaturatedDoesNotBlockShutdown(t *testing.T) {
	wc := newWsConn(nil)
	go wc.runSendDispatcher(logger.GetLogger())

	hold := make(chan struct{})
	slow := func() { <-hold }

	// Saturate all concurrency slots plus one more queued so the
	// dispatcher is blocked on sendSem when we trigger shutdown.
	for i := 0; i < wsConnSendConcurrency+1; i++ {
		wc.sendQueue <- slow
	}
	time.Sleep(50 * time.Millisecond) // let dispatcher reach the parked acquire

	close(wc.sendDone)
	<-wc.sendDispatcherDone
	// Release all jobs so sendWG can drain. The dispatcher exited
	// via the cancel-able select on its own — sendDispatcherDone
	// confirms that, ruling out a race between sendWG.Add and the
	// Wait below.
	close(hold)

	waitDone := make(chan struct{})
	go func() {
		wc.sendWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("sendWG.Wait hung after shutdown even though all jobs were released")
	}
}

// TestErrorResultsForTargets covers the helper used when the send queue
// overflows. The SDK keys StatusTracker by envelopeUuid → messageId,
// so each rejected target must echo its envelopeUuid back; a single
// flat error result wouldn't let the SDK downgrade individual
// messages to "failed".
func TestErrorResultsForTargets(t *testing.T) {
	targets := []SendTarget{
		{DeviceID: "d1", EnvelopeUUID: "u1", Ciphertext: "c1"},
		{DeviceID: "d2", EnvelopeUUID: "u2", Ciphertext: "c2"},
	}
	got := errorResultsForTargets(targets, "send_queue_full")
	if len(got) != 2 {
		t.Fatalf("len=%d; want 2", len(got))
	}
	for i, r := range got {
		if r.EnvelopeUUID != targets[i].EnvelopeUUID {
			t.Errorf("result[%d].EnvelopeUUID=%q; want %q", i, r.EnvelopeUUID, targets[i].EnvelopeUUID)
		}
		if r.Status != StatusError {
			t.Errorf("result[%d].Status=%q; want %q", i, r.Status, StatusError)
		}
		if r.Err != "send_queue_full" {
			t.Errorf("result[%d].Err=%q; want %q", i, r.Err, "send_queue_full")
		}
	}
}

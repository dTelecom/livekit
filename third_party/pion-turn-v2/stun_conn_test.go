//go:build !js
// +build !js

package turn

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/pion/stun"
	"github.com/stretchr/testify/assert"
)

// channelDataFrame builds a ChannelData frame (channel 0x4000) whose header
// declares dataLen bytes and carries that many payload bytes plus padding.
func channelDataFrame(dataLen int) []byte {
	padded := (dataLen + channelDataPadding - 1) &^ (channelDataPadding - 1)
	frame := make([]byte, channelDataHeaderSize+padded)
	binary.BigEndian.PutUint16(frame[0:2], 0x4000)
	binary.BigEndian.PutUint16(frame[2:4], uint16(dataLen))
	return frame
}

// A frame larger than the caller's buffer must be rejected, not reported
// with n > len(p): Server.readLoop slices buf[:n] and panicked on that.
func TestSTUNConnRejectsFrameLargerThanBuffer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close() //nolint:errcheck
	conn := NewSTUNConn(server)
	defer conn.Close() //nolint:errcheck

	small := channelDataFrame(8)
	go func() {
		_, _ = client.Write(channelDataFrame(21332)) // 21336-byte frame, as seen in prod
		_, _ = client.Write(small)
	}()

	buf := make([]byte, defaultInboundMTU)
	n, _, err := conn.ReadFrom(buf)
	assert.ErrorIs(t, err, errTURNFrameTooLarge)
	assert.Equal(t, 0, n)

	// The oversized frame is consumed, so framing stays aligned.
	n, _, err = conn.ReadFrom(buf)
	assert.NoError(t, err)
	assert.Equal(t, small, buf[:n])
}

// Length fields near 0xFFFF used to overflow the uint16 frame size to 0,
// making ReadFrom return (0, nil) forever without consuming data.
func TestSTUNConnLengthOverflow(t *testing.T) {
	stunHeader := make([]byte, stunHeaderSize)
	binary.BigEndian.PutUint16(stunHeader[0:2], 0x0001)
	binary.BigEndian.PutUint16(stunHeader[2:4], 0xFFEC) // 0xFFEC + 20 == 0x10000
	binary.BigEndian.PutUint32(stunHeader[4:8], 0x2112A442)

	channelHeader := make([]byte, 12)
	binary.BigEndian.PutUint16(channelHeader[0:2], 0x4000)
	binary.BigEndian.PutUint16(channelHeader[2:4], 0xFFFC) // 0xFFFC + 4 == 0x10000

	for name, header := range map[string][]byte{"stun": stunHeader, "channel data": channelHeader} {
		header := header
		t.Run(name, func(t *testing.T) {
			n, err := consumeSingleTURNFrame(header)
			assert.ErrorIs(t, err, errIncompleteTURNFrame)
			assert.Equal(t, 0, n)

			client, server := net.Pipe()
			conn := NewSTUNConn(server)
			defer conn.Close() //nolint:errcheck
			go func() {
				_, _ = client.Write(header)
				_ = client.Close()
			}()

			// The frame is incomplete, so ReadFrom waits for more data and
			// then sees EOF instead of returning an empty frame.
			n, _, err = conn.ReadFrom(make([]byte, defaultInboundMTU))
			assert.ErrorIs(t, err, io.EOF)
			assert.Equal(t, 0, n)
		})
	}
}

// End to end: an oversized frame on a TURN TCP listener must not crash the
// server, and the server must keep serving other connections.
func TestServerSurvivesOversizedTCPFrame(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	assert.NoError(t, err)

	server, err := NewServer(ServerConfig{
		AuthHandler: func(string, string, net.Addr) ([]byte, bool) { return nil, false },
		ListenerConfigs: []ListenerConfig{{
			Listener: listener,
			RelayAddressGenerator: &RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP("127.0.0.1"),
				Address:      "0.0.0.0",
			},
		}},
		Realm:         "pion.ly",
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	assert.NoError(t, err)
	defer server.Close() //nolint:errcheck

	bad, err := net.Dial("tcp4", listener.Addr().String())
	assert.NoError(t, err)
	_, err = bad.Write(channelDataFrame(21332))
	assert.NoError(t, err)
	defer bad.Close() //nolint:errcheck

	// Give the server's read loop time to process the frame.
	time.Sleep(100 * time.Millisecond)

	good, err := net.Dial("tcp4", listener.Addr().String())
	assert.NoError(t, err)
	defer good.Close() //nolint:errcheck
	assert.NoError(t, good.SetDeadline(time.Now().Add(5*time.Second)))

	req := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	_, err = good.Write(req.Raw)
	assert.NoError(t, err)

	buf := make([]byte, defaultInboundMTU)
	n, _, err := NewSTUNConn(good).ReadFrom(buf)
	assert.NoError(t, err)

	resp := &stun.Message{Raw: append([]byte{}, buf[:n]...)}
	assert.NoError(t, resp.Decode())
	assert.Equal(t, stun.BindingSuccess, resp.Type)
	assert.Equal(t, req.TransactionID, resp.TransactionID)
}

package service

import (
	"io"
	"net"
	"net/http"
	"time"

	"github.com/livekit/protocol/logger"
)

// newReverseProxyHandler creates an HTTP handler that proxies all connections
// (including WebSocket upgrades) to a backend TCP address.
//
// It works by hijacking the client connection after TLS termination and
// forwarding the raw HTTP request to the backend, then copying bytes
// bidirectionally. This transparently supports WebSocket upgrades.
func newReverseProxyHandler(targetAddr string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Connect to backend
		backendConn, err := net.DialTimeout("tcp", targetAddr, 10*time.Second)
		if err != nil {
			logger.Errorw("reverse proxy: backend dial failed", err, "target", targetAddr)
			http.Error(w, "Backend unavailable", http.StatusBadGateway)
			return
		}

		// Hijack client connection (already TLS-terminated by ServeTLS)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			logger.Errorw("reverse proxy: hijacking not supported", nil)
			http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
			backendConn.Close()
			return
		}

		clientConn, clientBuf, err := hijacker.Hijack()
		if err != nil {
			logger.Errorw("reverse proxy: hijack failed", err)
			backendConn.Close()
			return
		}

		// Forward the original HTTP request to the backend
		if err := r.Write(backendConn); err != nil {
			logger.Errorw("reverse proxy: failed to write request to backend", err)
			clientConn.Close()
			backendConn.Close()
			return
		}

		// Bidirectional copy: client ↔ backend
		errc := make(chan error, 2)

		go func() {
			_, err := io.Copy(backendConn, clientBuf)
			errc <- err
		}()

		go func() {
			_, err := io.Copy(clientConn, backendConn)
			errc <- err
		}()

		// Wait for either direction to finish, then close both
		<-errc
		clientConn.Close()
		backendConn.Close()
	})
}

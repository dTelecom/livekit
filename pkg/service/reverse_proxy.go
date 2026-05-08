package service

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/livekit/protocol/logger"
)

// startReverseProxy terminates TLS on the SNI-routed listener and proxies
// all traffic (including WebSocket upgrades) to the backend using the
// stdlib httputil.ReverseProxy.
//
// We use tls.NewListener + http.Server.Serve (NOT ServeTLS) so that HTTP/2
// is NOT enabled — this ensures Hijack() works for WebSocket upgrade proxying.
// httputil.ReverseProxy handles WebSocket upgrades natively since Go 1.12.
func startReverseProxy(listener net.Listener, targetAddr string, domain string, tlsConfig *tls.Config) {
	backendURL := &url.URL{
		Scheme: "http",
		Host:   targetAddr,
	}

	proxy := httputil.NewSingleHostReverseProxy(backendURL)

	// Wrap SNI listener with TLS (handshake happens in tls.NewListener)
	tlsListener := tls.NewListener(listener, tlsConfig)

	server := &http.Server{
		Handler: proxy,
	}

	// Serve (not ServeTLS) — TLS already handled by tls.NewListener.
	// This deliberately does NOT enable HTTP/2, so Hijack() works for
	// WebSocket upgrade proxying in httputil.ReverseProxy.
	if err := server.Serve(tlsListener); err != nil && err != http.ErrServerClosed {
		logger.Errorw("reverse proxy server error", err, "domain", domain)
	}
}

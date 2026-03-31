package service

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/inconshreveable/go-vhost"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/protocol/logger"
	"golang.org/x/crypto/acme/autocert"
)

// resilientListener wraps a net.Listener to handle transient Accept errors
// (e.g. EMFILE "too many open files") by logging and retrying instead of
// returning the error to the caller. This prevents the go-vhost muxer's
// run loop from exiting permanently on temporary errors.
type resilientListener struct {
	net.Listener
}

func (l *resilientListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				logger.Warnw("temporary accept error on port 443, retrying", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// permanent error (listener closed etc.) — pass through
			logger.Errorw("accept error on port 443", err)
			return nil, err
		}
		return conn, nil
	}
}

func NewCertManager(conf *config.Config) (*autocert.Manager, error) {
	if !conf.TURN.Enabled {
		return nil, nil
	}

	if conf.Domain != "" && conf.TURN.Domain != "" {
		certManager := autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(conf.Domain, conf.TURN.Domain),
		}

		dir := cacheDir()
		if dir != "" {
			certManager.Cache = autocert.DirCache(dir)
		}

		go http.ListenAndServe("0.0.0.0:80", certManager.HTTPHandler(nil))
		return &certManager, nil
	}
	return nil, fmt.Errorf("domains not set")
}

func NewVhostMuxer(conf *config.Config) (*vhost.TLSMuxer, error) {
	if !conf.TURN.Enabled {
		return nil, nil
	}

	if conf.Domain != "" && conf.TURN.Domain != "" {
		listener, err := net.Listen("tcp4", "0.0.0.0:443")

		if err != nil {
			return nil, err
		}

		return vhost.NewTLSMuxer(&resilientListener{listener}, 5*time.Second)
	} else {
		return nil, fmt.Errorf("domain or turn domain not set")
	}
}

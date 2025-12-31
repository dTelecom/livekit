package utils

import (
	"net/http"

	"github.com/livekit/protocol/logger"
)

type RequestLogger struct {
	log logger.Logger
}

func NewDebugRequestLogger() *RequestLogger {
	return &RequestLogger{
		log: logger.GetLogger(),
	}
}

func (m *RequestLogger) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	m.log.Debugw("http request received",
		"method", r.Method,
		"path", r.URL.Path,
		"query", r.URL.RawQuery,
		"remote", r.RemoteAddr,
		"userAgent", r.UserAgent(),
		"headers", r.Header,
	)

	next(w, r)
}
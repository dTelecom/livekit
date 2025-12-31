package utils

import (
	"net/http"

	"github.com/livekit/protocol/logger"
)

type RequestLogger struct {
	log logger.Logger
}

func NewRequestLogger() *RequestLogger {
	return &RequestLogger{
		log: logger.GetLogger(),
	}
}

// negroni.Handler
func (m *RequestLogger) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	m.log.Infow("http request received",
		"method", r.Method,
		"path", r.URL.Path,
		"query", r.URL.RawQuery,
		"remote", r.RemoteAddr,
		"userAgent", r.UserAgent(),
		"headers", r.Header,
	)

	next(w, r)
}
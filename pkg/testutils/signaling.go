package testutils

import (
	"errors"
	"net/url"
)

// SignalingOrigin normalizes a direct node URL for WebSocket signaling.
func SignalingOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("expected a node signaling origin")
	}
	switch u.Scheme {
	case "https", "wss":
		u.Scheme = "wss"
	case "http", "ws":
		u.Scheme = "ws"
	default:
		return "", errors.New("expected an HTTP or WebSocket origin")
	}
	u.Path = ""
	return u.String(), nil
}

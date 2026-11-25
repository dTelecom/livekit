package service_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/service"
)

func TestIsValidDomain(t *testing.T) {
	list := map[string]bool{
		"turn.myhost.com":  true,
		"turn.google.com":  true,
		"https://host.com": false,
		"turn://host.com":  false,
	}
	for key, result := range list {
		service.IsValidDomain(key)
		require.Equal(t, service.IsValidDomain(key), result)
	}
}

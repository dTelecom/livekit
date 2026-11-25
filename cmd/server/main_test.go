package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConfigString(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("fileContent"), 0o644))
	for _, tt := range []struct {
		file, body, want string
	}{
		{"", "", ""},
		{"", "configBody", "configBody"},
		{configFile, "configBody", "configBody"},
		{configFile, "", "fileContent"},
	} {
		body, err := getConfigString(tt.file, tt.body)
		require.NoError(t, err)
		require.Equal(t, tt.want, body)
	}
}

func TestShouldReturnErrorIfConfigFileDoesNotExist(t *testing.T) {
	configBody, err := getConfigString(filepath.Join(t.TempDir(), "missing.yaml"), "")
	require.Error(t, err)
	require.Empty(t, configBody)
}

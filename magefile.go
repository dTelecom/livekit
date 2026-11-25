//go:build mage
// +build mage

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/magefile/mage/mg"

	"github.com/livekit/livekit-server/version"
	"github.com/livekit/mageutil"
	_ "github.com/livekit/psrpc"
)

const (
	imageName = "livekit/livekit-server"
	// The pinned generators use an x/tools release compatible with Go 1.23.
	generatorToolchain = "GOTOOLCHAIN=go1.23.8"
)

var Default = Build

// Deps installs the Wire version selected by the repository's module graph.
func Deps() error {
	return runGo([]string{generatorToolchain}, "install", "-mod=readonly", "github.com/google/wire/cmd/wire")
}

// Build builds the server for the host using the checked-in generated sources.
func Build() error {
	return buildServer("bin/livekit-server")
}

// BuildLinux builds the static Linux/amd64 server.
func BuildLinux() error {
	return buildServer("bin/livekit-server-amd64", "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
}

func buildServer(output string, env ...string) error {
	if err := os.MkdirAll("bin", 0755); err != nil {
		return err
	}
	return runGo(env, "build", "-mod=readonly", "-o", output, "./cmd/server")
}

func runGo(env []string, args ...string) error {
	cmd := exec.Command(mg.GoCmd(), args...)
	cmd.Env = os.Environ()
	if os.Getenv("GOTOOLCHAIN") == "" {
		version, err := os.ReadFile(".go-version")
		if err != nil {
			return err
		}
		cmd.Env = append(cmd.Env, "GOTOOLCHAIN=go"+strings.TrimSpace(string(version)))
	}
	cmd.Env = append(cmd.Env, env...)
	mageutil.ConnectStd(cmd)
	return cmd.Run()
}

func Deadlock() error {
	ctx := context.Background()
	if err := mageutil.InstallTool("golang.org/x/tools/cmd/goimports", "latest", false); err != nil {
		return err
	}
	if err := mageutil.Run(ctx, "go get github.com/sasha-s/go-deadlock"); err != nil {
		return err
	}
	if err := mageutil.Pipe("grep -rl sync.Mutex ./pkg", "xargs sed -i  -e s/sync.Mutex/deadlock.Mutex/g"); err != nil {
		return err
	}
	if err := mageutil.Pipe("grep -rl sync.RWMutex ./pkg", "xargs sed -i  -e s/sync.RWMutex/deadlock.RWMutex/g"); err != nil {
		return err
	}
	if err := mageutil.Pipe("grep -rl deadlock.Mutex\\|deadlock.RWMutex ./pkg", "xargs goimports -w"); err != nil {
		return err
	}
	if err := mageutil.Run(ctx, "go mod tidy"); err != nil {
		return err
	}
	return nil
}

func Sync() error {
	if err := mageutil.Pipe("grep -rl deadlock.Mutex ./pkg", "xargs sed -i  -e s/deadlock.Mutex/sync.Mutex/g"); err != nil {
		return err
	}
	if err := mageutil.Pipe("grep -rl deadlock.RWMutex ./pkg", "xargs sed -i  -e s/deadlock.RWMutex/sync.RWMutex/g"); err != nil {
		return err
	}
	if err := mageutil.Pipe("grep -rl sync.Mutex\\|sync.RWMutex ./pkg", "xargs goimports -w"); err != nil {
		return err
	}
	if err := mageutil.Run(context.Background(), "go mod tidy"); err != nil {
		return err
	}
	return nil
}

// builds and publish snapshot docker image
func PublishDocker() error {
	// don't publish snapshot versions as latest or minor version
	if !strings.Contains(version.Version, "SNAPSHOT") {
		return errors.New("Cannot publish non-snapshot versions")
	}

	versionImg := fmt.Sprintf("%s:v%s", imageName, version.Version)
	cmd := exec.Command("docker", "buildx", "build",
		"--push", "--platform", "linux/amd64,linux/arm64",
		"--tag", versionImg,
		".")
	mageutil.ConnectStd(cmd)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// run unit tests, skipping integration
func Test() error {
	mg.Deps(setULimit)
	return runGo(nil, "test", "-mod=readonly", "-short", "-count=1", "./...")
}

// cleans up builds
func Clean() {
	fmt.Println("cleaning...")
	os.RemoveAll("bin")
}

// Generate runs the repository's pinned generators when explicitly requested.
func Generate() error {
	return runGo([]string{generatorToolchain}, "generate", "./...")
}

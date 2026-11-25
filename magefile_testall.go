//go:build mage

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/livekit/livekit-server/pkg/testutils"
	"github.com/magefile/mage/mg"
)

// TestAll runs the local suite and the integration suite on two registered test nodes.
// The last test drains node A. See "Running the full server suite" in README.md.
func TestAll() error {
	mg.Deps(setULimit)
	var missing []string
	for _, name := range []string{"DTELECOM_SMOKE_NODE_A", "DTELECOM_SMOKE_NODE_B", "DTELECOM_SMOKE_TOKEN_PROVIDER", "DTELECOM_CHAT_TOKEN_A_FILE", "DTELECOM_CHAT_TOKEN_B_FILE"} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		return fmt.Errorf("missing integration fixtures: %s (see Running the full server suite in README.md)", strings.Join(missing, ", "))
	}
	a, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_A"))
	if err != nil {
		return fmt.Errorf("DTELECOM_SMOKE_NODE_A: %w", err)
	}
	b, err := testutils.SignalingOrigin(os.Getenv("DTELECOM_SMOKE_NODE_B"))
	if err != nil {
		return fmt.Errorf("DTELECOM_SMOKE_NODE_B: %w", err)
	}
	if a == b {
		return fmt.Errorf("DTELECOM_SMOKE_NODE_A and DTELECOM_SMOKE_NODE_B must be distinct")
	}
	provider, err := exec.LookPath(os.Getenv("DTELECOM_SMOKE_TOKEN_PROVIDER"))
	if err != nil {
		return fmt.Errorf("DTELECOM_SMOKE_TOKEN_PROVIDER: %w", err)
	}
	for _, name := range []string{"DTELECOM_CHAT_TOKEN_A_FILE", "DTELECOM_CHAT_TOKEN_B_FILE"} {
		if err := requireGrantFile(os.Getenv(name)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	work, err := os.MkdirTemp("", "dtelecom-suite-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)

	// Use the short-lived chat grants first. Each later RTC case gets a fresh room.
	chat := filepath.Join(work, "chat.test")
	if err := runGo(nil, "test", "-mod=readonly", "-c", "-o", chat, "./pkg/chat"); err != nil {
		return err
	}
	if err := runJoinedCase(chat, "TestJoinedChatDeliveryAndAck", "", nil); err != nil {
		return err
	}
	// Run all package tests, including any long local cases. The joined chat case
	// already ran with its grants, and the RTC cases below use explicit flags.
	if err := runGo([]string{"DTELECOM_CHAT_TOKEN_A_FILE=", "DTELECOM_CHAT_TOKEN_B_FILE="},
		"test", "-mod=readonly", "-count=1", "./..."); err != nil {
		return err
	}
	rtc := filepath.Join(work, "rtc.test")
	if err := runGo(nil, "test", "-mod=readonly", "-c", "-o", rtc, "./test"); err != nil {
		return err
	}
	for _, suite := range []struct {
		name, flag string
		layouts    []string
	}{
		{"TestJoinedNodeSmoke", "-joined-smoke", nil},
		{"TestJoinedLegacyScenarios", "-joined-legacy", []string{"single", "mesh"}},
		{"TestJoinedMediaScenarios", "-joined-media", []string{"single", "mesh"}},
		{"TestJoinedParticipantLifecycle", "-joined-lifecycle", []string{"single", "mesh"}},
		{"TestJoinedRoomLifecycle", "-joined-rooms", []string{"single", "mesh"}},
		{"TestJoinedAdditionalParticipants", "-joined-participants", []string{"republish", "enable-subscribe"}},
		{"TestJoinedNodeDrain", "-joined-drain", nil},
	} {
		layouts := suite.layouts
		if len(layouts) == 0 {
			layouts = []string{""}
		}
		for _, layout := range layouts {
			name := suite.name
			if layout != "" {
				name += "/" + layout
			}
			if err := runRoomCase(work, provider, rtc, name, suite.flag); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	return nil
}

func requireGrantFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("expected a nonempty token file at %s", path)
	}
	return nil
}

func runRoomCase(work, provider, binary, name, flag string) error {
	dir, err := os.MkdirTemp(work, "room-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	fmt.Println("Issuing room grants for", name)
	cmd := exec.Command(provider, dir)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("room token provider: %w", err)
	}
	env := []string{"DTELECOM_SMOKE_ISSUER_KEY_FILE="}
	for variable, file := range map[string]string{
		"DTELECOM_SMOKE_TOKEN_A_FILE":          "a.jwt",
		"DTELECOM_SMOKE_TOKEN_B_FILE":          "b.jwt",
		"DTELECOM_SMOKE_TOKEN_C_FILE":          "c.jwt",
		"DTELECOM_SMOKE_RESTRICTED_TOKEN_FILE": "restricted.jwt",
		"DTELECOM_SMOKE_ADMIN_TOKEN_FILE":      "admin.jwt",
	} {
		path := filepath.Join(dir, file)
		if err := requireGrantFile(path); err != nil {
			return err
		}
		env = append(env, variable+"="+path)
	}
	return runJoinedCase(binary, name, flag, env)
}

func runJoinedCase(binary, name, flag string, env []string) error {
	filter := "^" + strings.ReplaceAll(name, "/", "$/^") + "$"
	args := []string{"-test.run", filter, "-test.v", "-test.timeout=4m"}
	if flag != "" {
		args = append(args, flag)
	}
	cmd := exec.Command(binary, args...)
	cmd.Env = append(os.Environ(), env...)
	var output bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &output) // Stream the drain marker to the operator.
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	// Go also exits successfully when a test is skipped or a filter matches nothing.
	passed := false
	for _, line := range strings.Split(output.String(), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "--- SKIP: ") {
			return fmt.Errorf("%s: %s", name, line)
		}
		if strings.HasPrefix(line, "--- PASS: "+name+" (") {
			passed = true
		}
	}
	if !passed {
		return fmt.Errorf("required integration case did not pass: %s", name)
	}
	return nil
}

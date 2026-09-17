//go:build !windows

package urnettools

import (
	"context"
	"io"
	"os/exec"
	"syscall"
)

// unixPlatform implements Platform for every non-windows GOOS. Process
// discovery differs between Linux (/proc scan) and macOS (pgrep) and lives
// in process_linux.go / process_darwin.go. The doc comment in the package
// header lists the documented divergences.
type unixPlatform struct {
	runner Runner
}

func newPlatform(runner Runner) Platform {
	return &unixPlatform{runner: runner}
}

// StartProvider launches `urnetwork provide` detached and in its own
// session so the child survives the tool's exit (faithful to Start-Process
// -WindowStyle Hidden). stdout/stderr go to /dev/null.
func (self *unixPlatform) StartProvider(ctx context.Context, binaryPath string) error {
	cmd := exec.CommandContext(ctx, binaryPath, "provide")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return nil
}

func (self *unixPlatform) FindProviderProcesses(ctx context.Context) ([]Process, error) {
	return findProviderProcesses(ctx, self.runner)
}

// KillProvider sends SIGTERM to each pid, ignoring every error including
// ESRCH (best-effort, matching Stop-Process -ErrorAction SilentlyContinue).
func (self *unixPlatform) KillProvider(ctx context.Context, procs []Process) error {
	for _, proc := range procs {
		_ = syscall.Kill(proc.PID, syscall.SIGTERM)
	}
	return nil
}

// RemovePathEntry is a no-op on Unix: the PS1's User-scope
// [Environment]::SetEnvironmentVariable throws on non-Windows .NET, and the
// shell installer owns ~/.bashrc. The Go tool never edits ~/.bashrc.
func (self *unixPlatform) RemovePathEntry(entry string) (bool, error) {
	return false, nil
}

// homeDir returns the current user's home directory, or "" on failure.
func homeDir() string {
	home, err := userHomeDir()
	if err != nil {
		return ""
	}
	return home
}

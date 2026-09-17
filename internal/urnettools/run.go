package urnettools

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// Runner runs a subprocess and returns its exit error. All platform
// subprocess work (systemctl, launchctl, pwsh, powershell.exe, taskkill,
// pgrep) goes through a Runner so tests can substitute a fake. The
// installer script for update/reinstall is also run through it. Output is
// captured for Output and streamed to the Tool's writers for Run.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) error
	// Output runs the command and returns its combined stdout. Used where
	// the caller must parse the command's output (pgrep). The interface
	// extends the arch's Run-only surface because all subprocess work,
	// including pgrep, must stay testable (.1).
	Output(ctx context.Context, name string, args ...string) (string, error)
	// RunDiscard runs the command discarding all output; it reports only the
	// exit status. Used for probes whose output must not leak to the user
	// (e.g. systemctl is-enabled).
	RunDiscard(ctx context.Context, name string, args ...string) error
}

// NewExecRunner returns a Runner backed by os/exec whose Run output is
// wired to the Tool's writers when the Tool is constructed.
func NewExecRunner() Runner {
	return &execRunner{
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
}

// writerSetter is the optional interface NewTool uses to route subprocess
// output through the Tool's Stdout/Stderr.
type writerSetter interface {
	SetWriters(stdout, stderr io.Writer)
}

type execRunner struct {
	stateLock sync.Mutex
	stdout    io.Writer
	stderr    io.Writer
}

// RunDiscard runs name with all output discarded; only the exit status is
// reported.
func (self *execRunner) RunDiscard(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// SetWriters replaces the output writers so installer/systemctl output
// flows through the Tool's buffers.
func (self *execRunner) SetWriters(stdout, stderr io.Writer) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.stdout = stdout
	self.stderr = stderr
}

func (self *execRunner) Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	self.stateLock.Lock()
	cmd.Stdout = self.stdout
	cmd.Stderr = self.stderr
	self.stateLock.Unlock()
	return cmd.Run()
}

func (self *execRunner) Output(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return stdout.String(), err
	}
	return stdout.String(), nil
}

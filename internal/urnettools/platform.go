package urnettools

import "context"

// Process is a running provider process, identified by pid. The tool's
// model is process-level (Get-Process/Stop-Process in the PS1), not
// service-level.
type Process struct {
	PID int
}

// Platform is the seam between shared command logic and OS code. Build
// tags select the implementation: platform_windows.go for windows,
// platform_unix.go for the rest (linux/darwin), with the auto-start and
// auto-update pieces split per OS (autostart_*_linux|darwin|windows and
// autoupdate_*_linux|darwin|windows).
type Platform interface {
	// Process control.
	StartProvider(ctx context.Context, binaryPath string) error
	FindProviderProcesses(ctx context.Context) ([]Process, error)
	KillProvider(ctx context.Context, procs []Process) error // best-effort; nil on ESRCH

	// Auto-start.
	AutoStartEnabled(install *Install) (bool, error)
	EnableAutoStart(ctx context.Context, install *Install) error
	DisableAutoStart(ctx context.Context, install *Install) error

	// Auto-update.
	AutoUpdateEnabled(install *Install) (bool, error)
	EnableAutoUpdate(ctx context.Context, install *Install, freq Frequency) error
	DisableAutoUpdate(ctx context.Context, install *Install) error

	// RemovePathEntry removes entry from the user PATH and reports whether
	// anything was removed. On Windows this is a User-scope registry write;
	// elsewhere it is a no-op. The `removed` result lets the shared
	// uninstall handler emit the PS1's `Updating %PATH%` line only when an
	// entry was actually present (a small extension of the arch's signature,
	// which returns only an error).
	RemovePathEntry(entry string) (bool, error)
}

// NewPlatform builds the Platform implementation for the current GOOS.
func NewPlatform(runner Runner) Platform {
	return newPlatform(runner)
}

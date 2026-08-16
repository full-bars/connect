//go:build linux

package urnettools

import (
	"context"
	"os"
	"path/filepath"
)

// systemdUserDir is the per-user systemd unit directory, matching the shell
// installer's layout. systemd user units need an active user session or
// `loginctl enable-linger`.
func systemdUserDir() string {
	return filepath.Join(homeDir(), ".config", "systemd", "user")
}

func (self *unixPlatform) AutoStartEnabled(install *Install) (bool, error) {
	// Discard status output: systemctl is-enabled prints "enabled"/"disabled"
	// which must not leak into the user-facing command output.
	err := self.runner.RunDiscard(context.Background(), "systemctl", "--user", "is-enabled", "urnetwork.service")
	return err == nil, nil
}

func (self *unixPlatform) EnableAutoStart(ctx context.Context, install *Install) error {
	serviceDir := systemdUserDir()
	if err := os.MkdirAll(serviceDir, 0o755); err != nil {
		return err
	}
	servicePath := filepath.Join(serviceDir, "urnetwork.service")
	if err := os.WriteFile(servicePath, []byte(renderSystemdService(install.BinaryPath())), 0o644); err != nil {
		return err
	}
	if err := self.runner.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	if err := self.runner.Run(ctx, "systemctl", "--user", "enable", "urnetwork.service"); err != nil {
		return err
	}
	return nil
}

func (self *unixPlatform) DisableAutoStart(ctx context.Context, install *Install) error {
	// systemctl disable may fail when the unit is not enabled; auto-start-
	// disable never errors, faithful to Disable-AutoStart.
	_ = self.runner.Run(ctx, "systemctl", "--user", "disable", "urnetwork.service")
	_ = os.Remove(filepath.Join(systemdUserDir(), "urnetwork.service"))
	return nil
}

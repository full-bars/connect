//go:build linux

package urnettools

import (
	"context"
	"os"
	"path/filepath"
)

func (self *unixPlatform) AutoUpdateEnabled(install *Install) (bool, error) {
	err := self.runner.RunDiscard(context.Background(), "systemctl", "--user", "is-enabled", "urnetwork-update.timer")
	return err == nil, nil
}

func (self *unixPlatform) EnableAutoUpdate(ctx context.Context, install *Install, freq Frequency) error {
	unitDir := systemdUserDir()
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return err
	}
	updateService := filepath.Join(unitDir, "urnetwork-update.service")
	updateTimer := filepath.Join(unitDir, "urnetwork-update.timer")
	if err := os.WriteFile(updateService, []byte(renderSystemdUpdateService(install.ToolsPath())), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(updateTimer, []byte(renderSystemdUpdateTimer(OnCalendar(freq))), 0o644); err != nil {
		return err
	}
	if err := self.runner.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	if err := self.runner.Run(ctx, "systemctl", "--user", "enable", "--now", "urnetwork-update.timer"); err != nil {
		return err
	}
	// Review finding #10: a timer with OnCalendar does not fire immediately,
	// but the PS1 launches the updater right away on enable; start the
	// oneshot service once to reproduce that immediate update check.
	if err := self.runner.Run(ctx, "systemctl", "--user", "start", "urnetwork-update.service"); err != nil {
		return err
	}
	return nil
}

func (self *unixPlatform) DisableAutoUpdate(ctx context.Context, install *Install) error {
	// best-effort disable; the units may not be enabled.
	_ = self.runner.Run(ctx, "systemctl", "--user", "disable", "--now", "urnetwork-update.timer")
	_ = os.Remove(filepath.Join(systemdUserDir(), "urnetwork-update.timer"))
	_ = os.Remove(filepath.Join(systemdUserDir(), "urnetwork-update.service"))
	return nil
}

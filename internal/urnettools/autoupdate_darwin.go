//go:build darwin

package urnettools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func updatePlistPath() string {
	return filepath.Join(launchAgentsDir(), "com.urnetwork.update.plist")
}

func (self *unixPlatform) AutoUpdateEnabled(install *Install) (bool, error) {
	_, err := os.Stat(updatePlistPath())
	return err == nil, nil
}

func (self *unixPlatform) EnableAutoUpdate(ctx context.Context, install *Install, freq Frequency) error {
	if err := os.MkdirAll(launchAgentsDir(), 0o755); err != nil {
		return err
	}
	plist := updatePlistPath()
	if err := os.WriteFile(plist, []byte(renderUpdatePlist(install.ToolsPath(), StartInterval(freq))), 0o644); err != nil {
		return err
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := self.runner.Run(ctx, "launchctl", "bootstrap", domain, plist); err != nil {
		if err := self.runner.Run(ctx, "launchctl", "load", "-w", plist); err != nil {
			// Remove the plist on failure so a stray file cannot make the next
			// attempt report "already enabled".
			_ = os.Remove(plist)
			return err
		}
	}
	return nil
}

func (self *unixPlatform) DisableAutoUpdate(ctx context.Context, install *Install) error {
	plist := updatePlistPath()
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := self.runner.Run(ctx, "launchctl", "bootout", domain, plist); err != nil {
		_ = self.runner.Run(ctx, "launchctl", "unload", "-w", plist)
	}
	_ = os.Remove(plist)
	return nil
}

//go:build darwin

package urnettools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func launchAgentsDir() string {
	return filepath.Join(homeDir(), "Library", "LaunchAgents")
}

// providerPlistPath is the auto-start LaunchAgent plist.
func providerPlistPath() string {
	return filepath.Join(launchAgentsDir(), "com.urnetwork.provider.plist")
}

func (self *unixPlatform) AutoStartEnabled(install *Install) (bool, error) {
	_, err := os.Stat(providerPlistPath())
	return err == nil, nil
}

func (self *unixPlatform) EnableAutoStart(ctx context.Context, install *Install) error {
	if err := os.MkdirAll(launchAgentsDir(), 0o755); err != nil {
		return err
	}
	plist := providerPlistPath()
	if err := os.WriteFile(plist, []byte(renderProviderPlist(install.BinaryPath(), install.Path)), 0o644); err != nil {
		return err
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := self.runner.Run(ctx, "launchctl", "bootstrap", domain, plist); err != nil {
		// launchctl bootstrap needs a GUI login session; fall back to the
		// legacy load for older macOS or headless login domains.
		if err := self.runner.Run(ctx, "launchctl", "load", "-w", plist); err != nil {
			// Remove the plist on failure: AutoStartEnabled reports state from
			// file presence, so a stray plist would make the next attempt say
			// "already enabled" while no job is loaded.
			_ = os.Remove(plist)
			return err
		}
	}
	return nil
}

func (self *unixPlatform) DisableAutoStart(ctx context.Context, install *Install) error {
	plist := providerPlistPath()
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	if err := self.runner.Run(ctx, "launchctl", "bootout", domain, plist); err != nil {
		_ = self.runner.Run(ctx, "launchctl", "unload", "-w", plist)
	}
	_ = os.Remove(plist)
	return nil
}

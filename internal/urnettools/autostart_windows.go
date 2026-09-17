//go:build windows

package urnettools

import (
	"context"
	"os"
	"path/filepath"
)

// windowsStartupDir is the per-user Startup folder used by the PS1's
// Add-StartupCommand (urnet-tools.ps1:131).
func windowsStartupDir() string {
	return filepath.Join(os.Getenv("APPDATA"), "Microsoft", "Windows", "Start Menu", "Programs", "Startup")
}

func (self *windowsPlatform) AutoStartEnabled(install *Install) (bool, error) {
	_, err := os.Stat(filepath.Join(windowsStartupDir(), "urnetwork.lnk"))
	return err == nil, nil
}

func (self *windowsPlatform) EnableAutoStart(ctx context.Context, install *Install) error {
	startupDir := windowsStartupDir()
	if err := os.MkdirAll(startupDir, 0o755); err != nil {
		return err
	}
	shortcut := filepath.Join(startupDir, "urnetwork.lnk")
	script := renderWindowsShortcutScript(shortcut, install.BinaryPath(), "provide", install.Path)
	// The .lnk is created by shelling out to PowerShell (the PS1's own
	// mechanism); the Go standard library has no COM/IShellLink wrapper, so
	// the WScript.Shell one-liner is the reliable cross-version path
	// (native COM preferred over a powershell.exe spawn).
	return self.runner.Run(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script)
}

func (self *windowsPlatform) DisableAutoStart(ctx context.Context, install *Install) error {
	startupDir := windowsStartupDir()
	// Both the auto-start and the auto-update shortcut are removed, faithful
	// to Disable-AutoStart (urnet-tools.ps1:226-238).
	_ = os.Remove(filepath.Join(startupDir, "urnetwork.lnk"))
	_ = os.Remove(filepath.Join(startupDir, "urnetwork-update.lnk"))
	return nil
}

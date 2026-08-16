//go:build windows

package urnettools

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
)

func (self *windowsPlatform) AutoUpdateEnabled(install *Install) (bool, error) {
	_, err := os.Stat(filepath.Join(windowsStartupDir(), "urnetwork-update.lnk"))
	return err == nil, nil
}

func (self *windowsPlatform) EnableAutoUpdate(ctx context.Context, install *Install, freq Frequency) error {
	startupDir := windowsStartupDir()
	if err := os.MkdirAll(startupDir, 0o755); err != nil {
		return err
	}
	shortcut := filepath.Join(startupDir, "urnetwork-update.lnk")
	arguments := fmt.Sprintf("-InstalledPath \"%s\" -Frequency every-%s", install.Path, freq)
	script := renderWindowsShortcutScript(shortcut, install.UpdaterPath(), arguments, install.Path)
	if err := self.runner.Run(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script); err != nil {
		return err
	}
	// Launch the updater now, faithful to Start-Process at urnet-tools.ps1:165.
	// Plain exec.Command, NOT CommandContext: the updater must outlive this
	// tool invocation (faithful to Start-Process at urnet-tools.ps1:165). A
	// cancellable ctx would kill the child when the parent exits.
	updater := exec.Command(install.UpdaterPath(), "-InstalledPath", install.Path, "-Frequency", "every-"+string(freq))
	updater.Stdout = io.Discard
	updater.Stderr = io.Discard
	updater.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: 0x08000000, // CREATE_NO_WINDOW
	}
	if err := updater.Start(); err != nil {
		return err
	}
	return nil
}

func (self *windowsPlatform) DisableAutoUpdate(ctx context.Context, install *Install) error {
	_ = os.Remove(filepath.Join(windowsStartupDir(), "urnetwork-update.lnk"))
	// Kill the running updater via its pid file, faithful to
	// urnet-tools.ps1:186-195: only when the pid file reads cleanly.
	if pid, err := install.ReadUpdaterPID(); err == nil {
		_ = os.Remove(install.UpdaterPIDPath())
		_ = self.runner.Run(ctx, "taskkill", "/F", "/PID", strconv.Itoa(pid))
	}
	return nil
}

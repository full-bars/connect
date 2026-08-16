package urnettools

import (
	"context"
	"errors"
	"fmt"
	"runtime"
)

// doAutoStartEnable is the `auto-start-enable` command (urnet-tools.ps1:
// 203-224). On Windows it prints the actual shortcut target
// (<install>\urnetwork.exe provide) rather than the PS1's powershell.exe
// indirection text, which would be misleading for a shortcut that targets
// the provider binary directly. On Linux/macOS nothing
// extra is printed (there is no powershell).
func (self *Tool) doAutoStartEnable(ctx context.Context) error {
	enabled, err := self.Platform.AutoStartEnabled(self.Install)
	if err != nil {
		return err
	}
	if enabled {
		return errors.New("Auto-start is already enabled")
	}
	if err := self.Platform.EnableAutoStart(ctx, self.Install); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		fmt.Fprintf(self.Stdout, "Startup command: %s provide\n", self.Install.BinaryPath())
	}
	return nil
}

// disableAutoStart is the `auto-start-disable` command (urnet-tools.ps1:
// 226-238): no output and no error even when auto-start was already
// disabled.
func (self *Tool) disableAutoStart(ctx context.Context) error {
	return self.Platform.DisableAutoStart(ctx, self.Install)
}

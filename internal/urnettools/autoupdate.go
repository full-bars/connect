package urnettools

import (
	"bufio"
	"io"
	"context"
	"errors"
	"fmt"
)

// doAutoUpdateEnable is the `auto-update-enable` command with the PS1's
// default frequency of day (urnet-tools.ps1:151-167).
func (self *Tool) doAutoUpdateEnable(ctx context.Context) error {
	return self.enableAutoUpdate(ctx, FrequencyDay)
}

// doAutoUpdateDisable is the `auto-update-disable` command: error when
// already disabled (urnet-tools.ps1:169-199, NoError=false).
func (self *Tool) doAutoUpdateDisable(ctx context.Context) error {
	return self.disableAutoUpdate(ctx, false)
}

// doAutoUpdateFreq is the `auto-update-freq` command (urnet-tools.ps1:
// 401-419): prompt on stdout (no trailing newline, like Read-Host), parse
// the answer, then disable and re-enable with the new frequency.
func (self *Tool) doAutoUpdateFreq(ctx context.Context) error {
	fmt.Fprint(self.Stdout, "How frequently do you want the update checks to happen? every-[day/week/month]")
	answer, err := bufio.NewReader(self.Stdin).ReadString('\n')
	if err != nil && !(err == io.EOF && answer != "") {
		// io.EOF with a non-empty final line (e.g. printf every-week |
		// urnet-tools auto-update-freq) is a complete answer, not an error
		//.
		return err
	}
	freq, ok := ParseFrequency(answer)
	if !ok {
		// upstream bug: $Frequency is undefined at urnet-tools.ps1:411, so
		// the message carries the empty value.
		return errors.New("Invalid frequency: ")
	}
	// PS1 semantics (urnet-tools.ps1:416): auto-update-freq calls
	// Disable-AutoUpdate with no -NoError, so an already-disabled install
	// errors here ("Auto update is already disabled!") and exits 1.
	// Reproduce that faithfully.
	if err := self.disableAutoUpdate(ctx, false); err != nil {
		return err
	}
	return self.enableAutoUpdate(ctx, freq)
}

// enableAutoUpdate checks idempotency, enables on the platform, and prints
// the PS1's `Auto update enabled (frequency: every <freq>)` line.
func (self *Tool) enableAutoUpdate(ctx context.Context, freq Frequency) error {
	enabled, err := self.Platform.AutoUpdateEnabled(self.Install)
	if err != nil {
		return err
	}
	if enabled {
		return errors.New("Auto update is already enabled!")
	}
	if err := self.Platform.EnableAutoUpdate(ctx, self.Install, freq); err != nil {
		return err
	}
	fmt.Fprintf(self.Stdout, "Auto update enabled (frequency: every %s)\n", freq)
	return nil
}

// disableAutoUpdate disables auto-update on the platform. With noError set
// (uninstall, auto-update-freq) an already-disabled state is a silent
// no-op; otherwise it is the `Auto update is already disabled!` error.
func (self *Tool) disableAutoUpdate(ctx context.Context, noError bool) error {
	enabled, err := self.Platform.AutoUpdateEnabled(self.Install)
	if err != nil {
		return err
	}
	if !enabled {
		if !noError {
			return errors.New("Auto update is already disabled!")
		}
		return nil
	}
	if err := self.Platform.DisableAutoUpdate(ctx, self.Install); err != nil {
		return err
	}
	fmt.Fprintln(self.Stdout, "Auto update disabled")
	return nil
}

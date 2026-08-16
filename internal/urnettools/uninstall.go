package urnettools

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// doUninstall is the `uninstall` command, the port of Do-Uninstall
// (urnet-tools.ps1:240-302). It removes individual files (not the install
// directory), the data directory, startup entries, and the install path
// from the user PATH. It succeeds (exit 0) even when nothing is installed.
func (self *Tool) doUninstall(ctx context.Context) error {
	fmt.Fprintln(self.Stdout, "Uninstalling URnetwork provider")
	if err := self.disableAutoUpdate(ctx, true); err != nil {
		return err
	}

	fmt.Fprintln(self.Stdout, "Removing provider executable")
	for _, path := range []string{
		self.Install.BinaryPath(),
		self.Install.ToolsPath(),
		self.Install.UpdaterPath(),
		self.Install.VersionPath(),
		self.Install.DatePath(),
	} {
		_ = os.Remove(path) // ignore not-found
	}

	fmt.Fprintln(self.Stdout, "Removing data directory")
	if dataDir := self.Install.DataDir(); dataDir != "" {
		_ = os.RemoveAll(dataDir) // ignore not-found
	}

	fmt.Fprintln(self.Stdout, "Removing startup entries (if any)")
	if err := self.disableAutoStart(ctx); err != nil {
		return err
	}

	// User PATH cleanup. Faithful to urnet-tools.ps1:287-301; on Unix this
	// is a no-op because [Environment]::SetEnvironmentVariable(..., User)
	// throws on non-Windows .NET, and the shell installer manages ~/.bashrc
	// itself (out of scope here — the Go tool never edits ~/.bashrc).
	removed, err := self.Platform.RemovePathEntry(self.Install.Path)
	if err != nil {
		return errors.New("Failed to update %PATH%")
	}
	if removed {
		fmt.Fprintln(self.Stdout, "Updating %PATH%")
	}
	return nil
}

// removePathEntry splits path on sep, drops the exact entry
// (case-insensitive, matching PowerShell's -contains) and empty entries,
// and rejoins on sep.
func removePathEntry(path, entry, sep string) string {
	var kept []string
	for _, part := range strings.Split(path, sep) {
		if part == "" || strings.EqualFold(part, entry) {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, sep)
}

//go:build windows

package urnettools

import (
	"context"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

// windowsPlatform implements Platform on Windows. Process enumeration uses
// the toolhelp snapshot API (
// locale-fragile). Auto-start/auto-update use Startup-folder.lnk shortcuts
// created via a PowerShell WScript.Shell one-liner (the PS1's own
// mechanism).
type windowsPlatform struct {
	runner Runner
}

func newPlatform(runner Runner) Platform {
	return &windowsPlatform{runner: runner}
}

// StartProvider launches `urnetwork provide` hidden and detached (faithful
// to Start-Process -WindowStyle Hidden). stdout/stderr go to /dev/null.
func (self *windowsPlatform) StartProvider(ctx context.Context, binaryPath string) error {
	cmd := exec.CommandContext(ctx, binaryPath, "provide")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: 0x08000000, // CREATE_NO_WINDOW
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	return nil
}

// FindProviderProcesses enumerates processes via CreateToolhelp32Snapshot
// and collects the pids whose image name is urnetwork.exe (case-
// insensitive), the equivalent of Get-Process | Where-Object ProcessName
// -eq 'urnetwork'.
func (self *windowsPlatform) FindProviderProcesses(ctx context.Context) ([]Process, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(snapshot)
	var entry windows.ProcessEntry32
	entry.Size = uint32(unsafe.Sizeof(entry))
	var procs []Process
	err = windows.Process32First(snapshot, &entry)
	for err == nil {
		name := windows.UTF16ToString(entry.ExeFile[:])
		if strings.EqualFold(name, "urnetwork.exe") {
			procs = append(procs, Process{PID: int(entry.ProcessID)})
		}
		err = windows.Process32Next(snapshot, &entry)
	}
	// ERROR_NO_MORE_FILES is normal end of enumeration, not a failure.
	// Any other error mid-loop is a real failure.
	if err != syscall.ERROR_NO_MORE_FILES {
		return nil, err
	}
	return procs, nil
}

// KillProvider force-kills each pid, ignoring errors (best-effort, matching
// Stop-Process -ErrorAction SilentlyContinue).
func (self *windowsPlatform) KillProvider(ctx context.Context, procs []Process) error {
	for _, proc := range procs {
		_ = self.runner.Run(ctx, "taskkill", "/F", "/PID", strconv.Itoa(proc.PID))
	}
	return nil
}

// RemovePathEntry removes entry from the User PATH (HKCU\Environment\Path)
// and reports whether an entry was removed. The value is written back as
// REG_EXPAND_SZ, preserving the type the installer uses. Faithful to
// Get-Path/Set-Path (urnet-tools.ps1:83-94).
func (self *windowsPlatform) RemovePathEntry(entry string) (bool, error) {
	key, err := registry.OpenKey(registry.CURRENT_USER, "Environment", registry.QUERY_VALUE|registry.SET_VALUE)
	if err != nil {
		return false, err
	}
	defer key.Close()
	current, _, err := key.GetStringValue("Path")
	if err != nil {
		if err == registry.ErrNotExist {
			return false, nil
		}
		return false, err
	}
	newPath := removePathEntry(current, entry, ";")
	if newPath == current {
		return false, nil
	}
	if err := key.SetExpandStringValue("Path", newPath); err != nil {
		return false, err
	}
	return true, nil
}

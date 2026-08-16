//go:build linux

package urnettools

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// findProviderProcesses scans /proc for processes whose comm is urnetwork
// (or urnetwork.exe for safety), the process-level equivalent of
// Get-Process | Where-Object ProcessName -eq 'urnetwork'. The runner
// parameter is accepted for a uniform per-OS signature; Linux discovery
// does not shell out.
func findProviderProcesses(ctx context.Context, runner Runner) ([]Process, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var procs []Process
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		commData, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "comm"))
		if err != nil {
			continue
		}
		name := strings.TrimSpace(string(commData))
		if name == "urnetwork" || name == "urnetwork.exe" {
			procs = append(procs, Process{PID: pid})
		}
	}
	return procs, nil
}

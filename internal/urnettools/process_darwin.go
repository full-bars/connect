//go:build darwin

package urnettools

import (
	"context"
	"strconv"
	"strings"
)

// findProviderProcesses uses `pgrep -x urnetwork` (and, if empty,
// `pgrep -x urnetwork.exe`) to find provider processes.
func findProviderProcesses(ctx context.Context, runner Runner) ([]Process, error) {
	pids, err := runPgrep(ctx, runner, "urnetwork")
	if err != nil {
		return nil, err
	}
	if len(pids) == 0 {
		pids, err = runPgrep(ctx, runner, "urnetwork.exe")
		if err != nil {
			return nil, err
		}
	}
	var procs []Process
	for _, pid := range pids {
		procs = append(procs, Process{PID: pid})
	}
	return procs, nil
}

// runPgrep captures pgrep output and parses the pids. pgrep exits 1 with
// no output when nothing matches, which is an empty result, not an error.
func runPgrep(ctx context.Context, runner Runner, name string) ([]int, error) {
	out, err := runner.Output(ctx, "pgrep", "-x", name)
	if err != nil {
		if strings.TrimSpace(out) == "" {
			return nil, nil
		}
		return nil, err
	}
	var pids []int
	for _, field := range strings.Fields(out) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

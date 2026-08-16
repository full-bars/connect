package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/urnetwork/connect/internal/urnettools"
)

func main() {
	frequency := flag.String("Frequency", "", "every-day|every-week|every-month")
	installedPath := flag.String("InstalledPath", "", "path where URnetwork provider was installed")
	flag.Parse()

	seconds, ok := urnettools.UpdaterFrequencySeconds(*frequency)
	if !ok {
		// Unlike auto-update-freq, the updater's switch does reference
		// $Frequency (it is a valid param), so the value appears in the
		// message (urnetwork-updater.ps1:12-15).
		fmt.Fprintf(os.Stderr, "Invalid frequency: %s\n", *frequency)
		os.Exit(1)
	}

	install, err := urnettools.ResolveInstall(*installedPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Overwrite the pid file, faithful to Set-Content $PIDFile "$PID"
	// (urnetwork-updater.ps1:25).
	if err := os.WriteFile(install.UpdaterPIDPath(), []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Loop forever: sleep the frequency, then run `urnet-tools update`
	// (urnetwork-updater.ps1:27-30). The updater does not re-check whether
	// auto-update is still enabled; disable kills it via the pid file.
	for {
		time.Sleep(time.Duration(seconds) * time.Second)
		cmd := exec.Command(install.ToolsPath(), "update")
		cmd.Dir = install.Path
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		_ = cmd.Run() // ignore exit code and errors
	}
}

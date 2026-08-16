package urnettools

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// HelpText is the concise help screen (arch section 3.3), printed for
// -h/-help/--help and exit 0.
const HelpText = `URnetwork manager and toolkit.

This tool helps you manage your URnetwork installation.

Subcommands:
  update              Update URnetwork
  uninstall           Uninstall URnetwork
  reinstall           Reinstall URnetwork
  status              Show provider status
  start               Start the provider
  stop                Stop the provider
  version             Show the version of your URnetwork installation
  auto-update-enable  Enable auto-updates
  auto-update-freq    Change the frequency of auto-update checks
  auto-update-disable Disable auto-updates
  auto-start-enable   Enable auto-start
  auto-start-disable  Disable auto-start

Options:
  -InstalledPath <path>  The path where URnetwork provider was installed.
                         Defaults to the directory containing this tool.
  -h, -help, --help      Show this help and exit.

Refer to https://docs.ur.io/provider for more help.
`

// validCommands is the PS1 ValidateSet (urnet-tools.ps1:55), lowercase.
var validCommands = []string{
	"uninstall",
	"update",
	"start",
	"stop",
	"status",
	"version",
	"reinstall",
	"auto-update-enable",
	"auto-update-disable",
	"auto-update-freq",
	"auto-start-enable",
	"auto-start-disable",
}

// IsCommand reports whether command (case-insensitive) is one of the 12.
func IsCommand(command string) bool {
	lower := strings.ToLower(command)
	for _, valid := range validCommands {
		if lower == valid {
			return true
		}
	}
	return false
}

// Tool is the command dispatcher. It never touches the OS or the network
// except through Platform, ReleaseClient and Runner, which makes every
// command testable with fakes. Stdin/Stdout/Stderr are injectable.
type Tool struct {
	Install  *Install
	Platform Platform
	Releases ReleaseClient
	Runner   Runner
	Stdout   io.Writer
	Stderr   io.Writer
	Stdin    io.Reader
}

// NewTool wires a Tool with real output streams and routes the runner's
// subprocess output through the tool's writers.
func NewTool(install *Install, platform Platform, releases ReleaseClient, runner Runner) *Tool {
	self := &Tool{
		Install:  install,
		Platform: platform,
		Releases: releases,
		Runner:   runner,
		Stdout:   os.Stdout,
		Stderr:   os.Stderr,
		Stdin:    os.Stdin,
	}
	if settable, ok := runner.(writerSetter); ok {
		settable.SetWriters(self.Stdout, self.Stderr)
	}
	return self
}

// Run dispatches to the command handler for command (case-insensitive). It
// returns nil on success or an error whose message is the PS1 Write-Error
// text. Unknown commands are normally rejected by main, but Run also guards.
func (self *Tool) Run(ctx context.Context, command string) error {
	switch strings.ToLower(command) {
	case "uninstall":
		return self.doUninstall(ctx)
	case "update":
		return self.doUpdate(ctx)
	case "start":
		return self.doStart(ctx)
	case "stop":
		return self.doStop(ctx)
	case "status":
		return self.doStatus(ctx)
	case "version":
		return self.doVersion(ctx)
	case "reinstall":
		return self.doReinstall(ctx)
	case "auto-update-enable":
		return self.doAutoUpdateEnable(ctx)
	case "auto-update-disable":
		return self.doAutoUpdateDisable(ctx)
	case "auto-update-freq":
		return self.doAutoUpdateFreq(ctx)
	case "auto-start-enable":
		return self.doAutoStartEnable(ctx)
	case "auto-start-disable":
		return self.disableAutoStart(ctx)
	}
	return fmt.Errorf("Invalid command: %s", command)
}

// doStart is the `start` command (urnet-tools.ps1:431-434): launch
// `urnetwork provide` detached and hidden; no guard against an already
// running provider (faithful to Start-Process, which would spawn a second
// instance).
func (self *Tool) doStart(ctx context.Context) error {
	if err := self.Platform.StartProvider(ctx, self.Install.BinaryPath()); err != nil {
		return fmt.Errorf("Failed to start: %v", err)
	}
	return nil
}

// doStop is the `stop` command (urnet-tools.ps1:436-445): kill every
// provider process, ignoring errors (Stop-Process -ErrorAction
// SilentlyContinue). Silent no-op when nothing is running.
func (self *Tool) doStop(ctx context.Context) error {
	procs, err := self.Platform.FindProviderProcesses(ctx)
	if err != nil {
		return err
	}
	_ = self.Platform.KillProvider(ctx, procs)
	return nil
}

// doStatus is the `status` command (urnet-tools.ps1:447-458).
func (self *Tool) doStatus(ctx context.Context) error {
	procs, err := self.Platform.FindProviderProcesses(ctx)
	if err != nil {
		return err
	}
	if len(procs) > 0 {
		fmt.Fprintln(self.Stdout, "Status: Running")
	} else {
		fmt.Fprintln(self.Stdout, "Status: Stopped")
	}
	return nil
}

// doVersion is the `version` command (urnet-tools.ps1:346-357). It reads
// the version file first, then CheckUpdate (which also reads the date
// file); a missing date file therefore fails this command even when the
// version file is present (upstream behavior, preserved).
func (self *Tool) doVersion(ctx context.Context) error {
	tag, err := self.Install.ReadVersion()
	if err != nil {
		return fmt.Errorf("Cannot read the version file at %s", self.Install.VersionPath())
	}
	fmt.Fprintf(self.Stdout, "URnetwork version %s\n", tag)
	_, err = self.CheckUpdate(ctx)
	return err
}

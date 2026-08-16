package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urnetwork/connect/internal/urnettools"
)

func main() {
	fs := flag.NewFlagSet("urnet-tools", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // we print our own errors
	var helpFlag bool
	fs.BoolVar(&helpFlag, "h", false, "show this help and exit")
	fs.BoolVar(&helpFlag, "help", false, "show this help and exit")
	fs.BoolVar(&helpFlag, "Help", false, "show this help and exit")
	installedPath := fs.String("InstalledPath", "", "path where URnetwork provider was installed")

	// PowerShell flags are case-insensitive; Go's flag is not. Normalize
	// any case variant of -InstalledPath to the canonical form before
	// parse, covering -flag=value and -flag value.
	args := os.Args[1:]
	for i, a := range args {
		if strings.HasPrefix(a, "-") {
			if name, val, hasVal := strings.Cut(strings.TrimLeft(a, "-"), "="); hasVal && strings.EqualFold(name, "InstalledPath") {
				args[i] = "-InstalledPath=" + val
			} else if strings.EqualFold(strings.TrimLeft(a, "-"), "InstalledPath") {
				args[i] = "-InstalledPath"
			}
		}
	}

	// stdlib flag stops at the first positional argument, but PowerShell
	// accepts the command before or after the flags (`urnet-tools update
	// -InstalledPath...`). When the first argument is a command, move it
	// behind the flags so both orderings work.
	if len(args) > 0 && urnettools.IsCommand(args[0]) {
		args = append(args[1:], args[0])
	}
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			fmt.Print(urnettools.HelpText)
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if helpFlag {
		fmt.Print(urnettools.HelpText)
		os.Exit(0)
	}

	positionals := fs.Args()
	var command string
	if len(positionals) > 0 {
		command = positionals[0]
	}
	if command == "" {
		fmt.Fprintln(os.Stderr, "Please specify a command!")
		os.Exit(1)
	}
	if !urnettools.IsCommand(command) {
		fmt.Fprintf(os.Stderr, "Invalid command: %s\n", command)
		os.Exit(1)
	}

	install, err := urnettools.ResolveInstall(*installedPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	runner := urnettools.NewExecRunner()
	tool := urnettools.NewTool(
		install,
		urnettools.NewPlatform(runner),
		urnettools.NewHTTPReleaseClient(urnettools.GithubAPIBase),
		runner,
	)
	if err := tool.Run(context.Background(), command); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

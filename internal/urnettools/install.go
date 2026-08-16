// Package urnettools implements the Go port of the upstream PowerShell
// manager script scripts/urnet-tools.ps1 plus the supporting
// cmd/urnetwork-updater worker. It is a faithful port: the same 12
// subcommands, the same observable output strings, the same exit codes,
// and the same upstream bugs (preserved verbatim with `// upstream bug`
// comments). See URNET_TOOLS_PORT_ARCH.md for the complete architecture.
//
// The package depends only on the Go standard library and golang.org/x/sys.
// It deliberately does not import the module root package
// (github.com/urnetwork/connect) nor any fork-specific machinery: no
// targeting, no grading, no proxy management, no subnet claims.
//
// Platform model: detection is by build tag (GOOS), not by a runtime
// switch. The Platform interface is the single seam between shared command
// logic and OS code. Process control stays process-level (start/stop/status
// via exec, /proc or the Windows toolhelp snapshot API), matching the PS1's
// Get-Process/Start-Process/Stop-Process model rather than a service
// manager; on Linux this deliberately diverges from the shell toolchain's
// systemctl-centric approach (.2).
//
// Documented divergences, per the ):
//  - Install layout: the Go port targets the flat layout
//   (I/urnetwork, I/urnet-tools, I/urnetwork-updater, I/version,
//   I/date, I/urnetwork-updater.pid). It does not read the shell
//   toolchain's dot-prefixed.version/.date or its bin/ subdirectory.
//  - macOS binary suffix: the PS1 gives macOS an.exe suffix
//   (if (-not $IsLinux) is true on macOS), which is broken. This port
//   uses runtime.GOOS, so macOS gets no suffix.
//  - On Unix, update/reinstall run Provider_Install_Linux.sh via sh, not
//   the PowerShell installer via pwsh (finding #1).
//  - Windows process enumeration uses CreateToolhelp32Snapshot, not
//   tasklist /FO CSV which is locale-fragile (finding #2).
//  - The downloaded installer script is verified against a pinned SHA-256
//   before execution (finding #3).
//  - The installer temp file is created with os.CreateTemp (unpredictable
//   name), not a fixed predictable path (finding #5).
//  - InstalledPath default resolution walks up from the resolved
//   executable directory looking for the flat install signature
//   (urnetwork binary + version file), per finding #4.
//  - auto-start on Windows targets the provider binary directly and prints
//   the real shortcut target rather than the PS1's powershell.exe
//   indirection (finding #11).
//  - systemd user units (Linux) and LaunchAgents (macOS) require an active
//   user session; systemd user units additionally need a logged-in
//   session or `loginctl enable-linger` (finding #14).
package urnettools

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// executable is injectable so ResolveInstall is testable; it returns the
// path of the running binary.
var executable = os.Executable

// userHomeDir is injectable so DataDir is testable without touching a real
// home directory.
var userHomeDir = os.UserHomeDir

// Layout describes which install layout a directory uses. The PS1 tool
// and its Windows installer use the flat layout: urnetwork[.exe], version,
// date at the install root. The Linux shell toolchain (Provider_Install_Linux.sh)
// uses the dot layout: bin/urnetwork,.version,.date. The tool is
// layout-aware so it works with whichever installer actually produced the
// install.
type Layout int

const (
	LayoutFlat Layout = iota
	LayoutDot
)

// Install locates the provider installation directory. All path accessors
// are pure (no I/O) except the Read* helpers.
type Install struct {
	Path   string // absolute install directory
	Layout Layout // detected install layout
}

// ResolveInstall maps the -InstalledPath flag (or its absence) to an
// *Install. An explicit value is used verbatim (absolutized); otherwise the
// directory of the running executable is used.
func ResolveInstall(explicit string) (*Install, error) {
	exePath, err := executable()
	if err != nil {
		return nil, fmt.Errorf("cannot resolve the running executable path: %w", err)
	}
	return resolveInstall(explicit, exePath), nil
}

// resolveInstall is the pure half of ResolveInstall.
//
// The default resolution walks up from the executable directory looking for
// the flat install signature (a sibling urnetwork binary plus a version
// file). os.Executable resolves symlinks while the PS1's Split-Path does
// not, so on a symlinked PATH install this walk finds the real install
// directory. If no directory in the chain carries the
// signature, the executable directory itself is used (the PS1's behavior).
func resolveInstall(explicit, exePath string) *Install {
	if explicit != "" {
		abs, err := filepath.Abs(explicit)
		if err != nil {
			return &Install{Path: filepath.Clean(explicit)}
		}
		return &Install{Path: abs, Layout: detectLayout(abs)}
	}
	exeDir := filepath.Dir(exePath)
	for dir := exeDir; ; {
		if layout, ok := installLayoutAt(dir); ok {
			return &Install{Path: dir, Layout: layout}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return &Install{Path: exeDir, Layout: detectLayout(exeDir)}
}

// detectLayout returns the layout for dir, defaulting to flat when neither
// signature is present (the flat layout is the PS1's own, so it is the
// sensible default for a fresh directory).
func detectLayout(dir string) Layout {
	if layout, ok := installLayoutAt(dir); ok {
		return layout
	}
	return LayoutFlat
}

// installLayoutAt reports whether dir is a flat or dot install directory,
// preferring flat when both are present.
func installLayoutAt(dir string) (Layout, bool) {
	flatBin := filepath.Join(dir, "urnetwork"+binarySuffixFor(runtime.GOOS))
	if info, err := os.Stat(flatBin); err == nil && !info.IsDir() {
		if info2, err := os.Stat(filepath.Join(dir, "version")); err == nil && !info2.IsDir() {
			return LayoutFlat, true
		}
	}
	dotBin := filepath.Join(dir, "bin", "urnetwork")
	if info, err := os.Stat(dotBin); err == nil && !info.IsDir() {
		if info2, err := os.Stat(filepath.Join(dir, ".version")); err == nil && !info2.IsDir() {
			return LayoutDot, true
		}
	}
	return LayoutFlat, false
}



// binarySuffixFor returns ".exe" for windows and "" otherwise, the faithful
// port of urnet-tools.ps1:75-79. It is split from binarySuffix so the
// windows branch is testable on any host.
func binarySuffixFor(goos string) string {
	if goos == "windows" {
		return ".exe"
	}
	return ""
}

func binarySuffix() string {
	return binarySuffixFor(runtime.GOOS)
}

func (self *Install) VersionPath() string {
	if self.Layout == LayoutDot {
		return filepath.Join(self.Path, ".version")
	}
	return filepath.Join(self.Path, "version")
}

func (self *Install) DatePath() string {
	if self.Layout == LayoutDot {
		return filepath.Join(self.Path, ".date")
	}
	return filepath.Join(self.Path, "date")
}

func (self *Install) BinaryPath() string {
	if self.Layout == LayoutDot {
		return filepath.Join(self.Path, "bin", "urnetwork")
	}
	return filepath.Join(self.Path, "urnetwork"+binarySuffix())
}

func (self *Install) ToolsPath() string {
	return filepath.Join(self.Path, "urnet-tools"+binarySuffix())
}

func (self *Install) UpdaterPath() string {
	return filepath.Join(self.Path, "urnetwork-updater"+binarySuffix())
}

func (self *Install) UpdaterPIDPath() string {
	return filepath.Join(self.Path, "urnetwork-updater.pid")
}

// DataDir is ~/.urnetwork on every platform. The PS1 branches on an
// undefined $OS, so its else branch (~/.urnetwork) always runs; on Windows
// that equals the dead branch's %HOMEDRIVE%%HOMEPATH%\.urnetwork, so
// unifying to os.UserHomeDir/.urnetwork is faithful in effect
// (upstream bug #3).
func (self *Install) DataDir() string {
	home, err := userHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".urnetwork")
}

// ReadVersion returns the trimmed installed release tag, or an error when
// the file is missing or empty (
// `-not $InstalledTag` check).
func (self *Install) ReadVersion() (string, error) {
	data, err := os.ReadFile(self.VersionPath())
	if err != nil {
		return "", err
	}
	tag := strings.TrimSpace(string(data))
	if tag == "" {
		return "", fmt.Errorf("version file at %s is empty", self.VersionPath())
	}
	return tag, nil
}

// ReadDate parses the installed release date (RFC 3339). Missing or
// unparseable contents are errors.
func (self *Install) ReadDate() (time.Time, error) {
	data, err := os.ReadFile(self.DatePath())
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, strings.TrimSpace(string(data)))
}

// ReadUpdaterPID parses the running updater's pid from the transient pid
// file.
func (self *Install) ReadUpdaterPID() (int, error) {
	data, err := os.ReadFile(self.UpdaterPIDPath())
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

package urnettools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"
)

// Installer URLs and pinned integrity hashes. The URLs point at
// refs/heads/main; the downloaded script is verified against the pinned
// SHA-256 before execution. The hashes were captured at
// port time:
//
//	Provider_Install_Win32.ps1: 58215ed3f9089b35bfecb66549e07ca3e5e98ae1db32b056f4453a8a7f30f089
//	Provider_Install_Linux.sh: b03f666269ee3af6803903b9ef4bc447103000cde2cc3d9979d316c49c408f58
//
// They are vars (not consts) so tests can substitute an httptest server and
// a freshly computed hash.
var (
	// Installer URLs pinned to an immutable commit SHA:
	// #3): the SHA-256 pins below are only sound if the URL cannot change
	// under them. refs/heads/main is mutable; any upstream edit to either
	// installer would stale the hash and break every update. Pin to the
	// commit these hashes were taken from so the URL and hash always agree.
	// When upstream changes an installer, bump BOTH the URL and the hash
	// together. NOTE: the provider binary downloaded INSIDE the installer is
	// NOT integrity-verified by this tool; only the installer script itself
	// is pinned.
	installerURLWin32 = "https://raw.githubusercontent.com/urnetwork/connect/4c85408b2311643ac203935911f690a783f63f25/scripts/Provider_Install_Win32.ps1"
	installerURLUnix  = "https://raw.githubusercontent.com/urnetwork/connect/4c85408b2311643ac203935911f690a783f63f25/scripts/Provider_Install_Linux.sh"
	installerSHAWin32 = "58215ed3f9089b35bfecb66549e07ca3e5e98ae1db32b056f4453a8a7f30f089"
	installerSHAUnix  = "b03f666269ee3af6803903b9ef4bc447103000cde2cc3d9979d316c49c408f58"
)

const (
	// installerMaxBytes caps the downloaded installer script size (1 MiB).
	// The real installers are a few KB.
	installerMaxBytes = 1 << 20
	// installerTempPattern is the os.CreateTemp pattern for the downloaded
	// installer; on windows the.ps1 suffix is required by powershell -File.
	installerTempPattern = "urnetwork-installer-*"
	// installerDownloadTimeout bounds the installer download.
	installerDownloadTimeout = 60 * time.Second
)

// installerSpec describes how to fetch and run the installer script on the
// current platform.
type installerSpec struct {
	host   string // pwsh/powershell.exe on windows, sh elsewhere
	url    string
	sha256 string
}

// CheckUpdate returns the latest tag if an update is available, or ("", nil)
// when the installed release is at least as recent. It is the sole place
// the tool reads the date file (urnet-tools.ps1:99-124). The comparison is
// date-based, not semver-based, and is preserved exactly.
func (self *Tool) CheckUpdate(ctx context.Context) (string, error) {
	release, err := self.Releases.Latest(ctx)
	if err != nil {
		return "", errors.New("Failed to fetch release information from GitHub API. Are you sure the version exists and your internet connection is working?")
	}
	installedDate, err := self.Install.ReadDate()
	if err != nil {
		return "", fmt.Errorf("Cannot read the installation date file at %s", self.Install.DatePath())
	}
	// PS1 semantics (urnet-tools.ps1:109-112): $? reflects the LAST command,
	// which is the VERSION read, not the date read. So a missing/empty
	// version file with a present date file DOES error here, with the
	// misleading "Cannot read the installation date file" message. Reproduce
	// that faithfully.
	installedTag, err := self.Install.ReadVersion()
	if err != nil {
		return "", fmt.Errorf("Cannot read the installation date file at %s", self.Install.DatePath())
	}
	if !installedDate.Before(release.PublishedAt) {
		fmt.Fprintf(self.Stdout, "Installed version is up-to-date (%s)\n", installedTag)
		return "", nil
	}
	fmt.Fprintf(self.Stdout, "Update available (%s)\n", release.TagName)
	return release.TagName, nil
}

// doUpdate is the `update` command (urnet-tools.ps1:315-344).
func (self *Tool) doUpdate(ctx context.Context) error {
	tag, err := self.CheckUpdate(ctx)
	if err != nil {
		return err
	}
	if tag == "" {
		fmt.Fprintln(self.Stdout, "No update is available")
		return nil
	}
	fmt.Fprintf(self.Stdout, "Downloading the installer script of version %s\n", tag)
	fmt.Fprintf(self.Stdout, "Executing installer script of version %s\n", tag)
	return self.runInstaller(ctx, tag, false)
}

// doReinstall is the `reinstall` command (urnet-tools.ps1:359-389).
func (self *Tool) doReinstall(ctx context.Context) error {
	tag, err := self.Install.ReadVersion()
	if err != nil {
		return fmt.Errorf("Cannot read the version file at %s", self.Install.VersionPath())
	}
	if err := self.doUninstall(ctx); err != nil {
		return err
	}
	fmt.Fprintf(self.Stdout, "Downloading the installer script of version %s\n", tag)
	// upstream bug: $InstallerTag is undefined at urnet-tools.ps1:369, so
	// the version prints as the empty string.
	fmt.Fprintln(self.Stdout, "Executing installer script of version ")
	return self.runInstaller(ctx, tag, true)
}

// runInstaller downloads and executes the platform installer script. It
// verifies the downloaded bytes against the pinned SHA-256 and runs them
// through the Runner before reporting the PS1's per-command completion or
// failure message.
func (self *Tool) runInstaller(ctx context.Context, tag string, reinstall bool) error {
	spec, err := resolveInstallerScript()
	if err != nil {
		return err
	}
	tempPath, err := downloadInstaller(ctx, spec.url, spec.sha256)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath) // best effort
	args := installerArgs(tempPath, self.Install.Path, tag, reinstall)
	if err := self.Runner.Run(ctx, spec.host, args...); err != nil {
		if reinstall {
			return errors.New("Reinstallation failed")
		}
		return errors.New("Update failed")
	}
	if reinstall {
		fmt.Fprintln(self.Stdout, "Reinstallation completed")
	} else {
		fmt.Fprintln(self.Stdout, "Update completed")
	}
	return nil
}

// resolveInstallerScript picks the host, URL and pinned hash for the
// current platform. Windows runs the PowerShell installer via pwsh (or
// powershell.exe); Unix runs Provider_Install_Linux.sh via sh (review
// finding #1 — forcing pwsh on Unix would break the seamless-transition
// promise).
func resolveInstallerScript() (*installerSpec, error) {
	if runtime.GOOS == "windows" {
		host, err := findPowerShell()
		if err != nil {
			return nil, err
		}
		return &installerSpec{host: host, url: installerURLWin32, sha256: installerSHAWin32}, nil
	}
	return &installerSpec{host: "sh", url: installerURLUnix, sha256: installerSHAUnix}, nil
}

// findPowerShell resolves the interpreter host used to run the PowerShell
// installer, preferring pwsh over the inbox powershell.exe.
func findPowerShell() (string, error) {
	if pwsh, err := exec.LookPath("pwsh"); err == nil {
		return pwsh, nil
	}
	if powershell, err := exec.LookPath("powershell.exe"); err == nil {
		return powershell, nil
	}
	return "", errors.New("pwsh is required to run the installer script")
}

// installerArgs builds the interpreter invocation. Windows matches the PS1
// (`& $TempScriptPath -Destination $InstalledPath -NonInteractive
// [-Version $tag]`). Unix uses the shell installer's actual flags:
// `-i/--install <dir>` for the destination, and for reinstall the
// `reinstall -t <tag> -B` operation form. The arch's literal
// `--destination/--non-interactive` do not exist in Provider_Install_Linux.sh
// (they would be rejected as invalid options), so the script's real flags
// are used instead. `-B` keeps the shell installer from editing ~/.bashrc,
// which is the Go tool's documented non-goal.
func installerArgs(tempPath, installDir, tag string, reinstall bool) []string {
	// Faithful to the PS1: `update` passes NO -Version (urnet-tools.ps1:334);
	// only `reinstall` passes -Version <installedTag> (:372). The installer
	// skips auto-update-enable whenever -Version is present, so passing it on
	// update would silently disable auto-update.
	if runtime.GOOS == "windows" {
		args := []string{"-NoProfile", "-File", tempPath, "-Destination", installDir, "-NonInteractive"}
		if reinstall && tag != "" {
			args = append(args, "-Version", tag)
		}
		return args
	}
	args := []string{tempPath, "--install", installDir}
	if reinstall && tag != "" {
		args = append(args, "reinstall", "-t", tag, "-B")
	}
	return args
}

// downloadInstaller fetches the installer script into a fresh temp file
// (os.CreateTemp, unpredictable name — ) and verifies its
// SHA-256 against the pinned hash. Any failure maps to the PS1's
// `Failed to download the installer script` message.
func downloadInstaller(ctx context.Context, url, wantSHA string) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, installerDownloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", errors.New("Failed to download the installer script")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.New("Failed to download the installer script")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", errors.New("Failed to download the installer script")
	}
	pattern := installerTempPattern + ".sh"   // sh accepts any name; .sh is clearer
	if runtime.GOOS == "windows" {
		pattern = installerTempPattern + ".ps1"
	}
	tempFile, err := os.CreateTemp(os.TempDir(), pattern)
	if err != nil {
		return "", errors.New("Failed to download the installer script")
	}
	hash := sha256.New()
	// Bound the download: the installers are a few KB; a misconfigured or
	// hostile endpoint must not fill the temp filesystem before the SHA
	// check rejects it.
	if _, err := io.Copy(io.MultiWriter(tempFile, hash), io.LimitReader(resp.Body, installerMaxBytes)); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return "", errors.New("Failed to download the installer script")
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		return "", errors.New("Failed to download the installer script")
	}
	if hex.EncodeToString(hash.Sum(nil)) != wantSHA {
		os.Remove(tempFile.Name())
		return "", errors.New("Failed to download the installer script")
	}
	return tempFile.Name(), nil
}

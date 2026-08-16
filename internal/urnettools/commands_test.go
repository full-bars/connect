package urnettools

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// fakeReleaseClient returns a canned release or error for CheckUpdate.
type fakeReleaseClient struct {
	latest *Release
	err    error
}

func (self *fakeReleaseClient) Latest(ctx context.Context) (*Release, error) {
	return self.latest, self.err
}

// fakeRunner records invocations and returns canned output/errors.
type fakeRunner struct {
	calls  [][]string
	output string
	err    error
}

func (self *fakeRunner) Run(ctx context.Context, name string, args ...string) error {
	self.calls = append(self.calls, append([]string{name}, args...))
	return self.err
}

func (self *fakeRunner) RunDiscard(ctx context.Context, name string, args ...string) error {
	self.calls = append(self.calls, append([]string{name}, args...))
	return self.err
}

func (self *fakeRunner) Output(ctx context.Context, name string, args ...string) (string, error) {
	self.calls = append(self.calls, append([]string{name}, args...))
	return self.output, self.err
}

// fakePlatform records which Platform methods ran and returns canned
// results.
type fakePlatform struct {
	startProviderErr     error
	findProcs            []Process
	findErr              error
	autoStartEnabledVal  bool
	autoStartEnabledErr  error
	autoUpdateEnabledVal bool
	autoUpdateEnabledErr error
	enableAutoStartErr   error
	disableAutoStartErr  error
	enableAutoUpdateErr  error
	disableAutoUpdateErr error
	removePathRemoved    bool
	removePathErr        error

	startProviderCalled     bool
	killProviderCalled      bool
	enableAutoStartCalled   bool
	disableAutoStartCalled  bool
	enableAutoUpdateCalled  bool
	disableAutoUpdateCalled bool
	lastFreq                Frequency
}

func (self *fakePlatform) StartProvider(ctx context.Context, binaryPath string) error {
	self.startProviderCalled = true
	return self.startProviderErr
}

func (self *fakePlatform) FindProviderProcesses(ctx context.Context) ([]Process, error) {
	return self.findProcs, self.findErr
}

func (self *fakePlatform) KillProvider(ctx context.Context, procs []Process) error {
	self.killProviderCalled = true
	return nil
}

func (self *fakePlatform) AutoStartEnabled(install *Install) (bool, error) {
	return self.autoStartEnabledVal, self.autoStartEnabledErr
}

func (self *fakePlatform) EnableAutoStart(ctx context.Context, install *Install) error {
	self.enableAutoStartCalled = true
	return self.enableAutoStartErr
}

func (self *fakePlatform) DisableAutoStart(ctx context.Context, install *Install) error {
	self.disableAutoStartCalled = true
	return self.disableAutoStartErr
}

func (self *fakePlatform) AutoUpdateEnabled(install *Install) (bool, error) {
	return self.autoUpdateEnabledVal, self.autoUpdateEnabledErr
}

func (self *fakePlatform) EnableAutoUpdate(ctx context.Context, install *Install, freq Frequency) error {
	self.enableAutoUpdateCalled = true
	self.lastFreq = freq
	self.autoUpdateEnabledVal = true
	return self.enableAutoUpdateErr
}

func (self *fakePlatform) DisableAutoUpdate(ctx context.Context, install *Install) error {
	self.disableAutoUpdateCalled = true
	self.autoUpdateEnabledVal = false
	return self.disableAutoUpdateErr
}

func (self *fakePlatform) RemovePathEntry(entry string) (bool, error) {
	return self.removePathRemoved, self.removePathErr
}

// newTestTool builds a Tool with buffer writers and an injected stdin.
func newTestTool(install *Install, platform Platform, releases ReleaseClient, runner Runner, stdin string) (*Tool, *bytes.Buffer, *bytes.Buffer) {
	var stdout, stderr bytes.Buffer
	return &Tool{
		Install:  install,
		Platform: platform,
		Releases: releases,
		Runner:   runner,
		Stdout:   &stdout,
		Stderr:   &stderr,
		Stdin:    strings.NewReader(stdin),
	}, &stdout, &stderr
}

func TestIsCommand(t *testing.T) {
	for _, cmd := range validCommands {
		if !IsCommand(cmd) {
			t.Errorf("IsCommand(%q) = false", cmd)
		}
	}
	if !IsCommand("UPDATE") {
		t.Error("IsCommand should be case-insensitive")
	}
	if IsCommand("bogus") {
		t.Error("IsCommand(bogus) = true")
	}
}

func TestRunStatusRunning(t *testing.T) {
	platform := &fakePlatform{findProcs: []Process{{PID: 42}}}
	tool, stdout, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "status"); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "Status: Running\n" {
		t.Errorf("stdout = %q, want %q", stdout.String(), "Status: Running\n")
	}
}

func TestRunStatusStopped(t *testing.T) {
	platform := &fakePlatform{}
	tool, stdout, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "status"); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "Status: Stopped\n" {
		t.Errorf("stdout = %q, want %q", stdout.String(), "Status: Stopped\n")
	}
}

func TestRunStart(t *testing.T) {
	platform := &fakePlatform{}
	tool, _, _ := newTestTool(&Install{Path: "/opt/urnetwork"}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "start"); err != nil {
		t.Fatal(err)
	}
	if !platform.startProviderCalled {
		t.Error("start should call StartProvider")
	}
}

func TestRunStartFailure(t *testing.T) {
	platform := &fakePlatform{startProviderErr: errors.New("boom")}
	tool, _, _ := newTestTool(&Install{Path: "/opt/urnetwork"}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "start")
	if err == nil || err.Error() != "Failed to start: boom" {
		t.Errorf("err = %v, want Failed to start: boom", err)
	}
}

func TestRunStop(t *testing.T) {
	platform := &fakePlatform{findProcs: []Process{{PID: 7}}}
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "stop"); err != nil {
		t.Fatal(err)
	}
	if !platform.killProviderCalled {
		t.Error("stop should call KillProvider")
	}
}

func TestRunVersion(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.VersionPath(), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(install.DatePath(), []byte("2025-01-02T03:04:05Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	releases := &fakeReleaseClient{latest: &Release{TagName: "v3.24.0", PublishedAt: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)}}
	tool, stdout, _ := newTestTool(install, &fakePlatform{}, releases, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "version"); err != nil {
		t.Fatal(err)
	}
	want := "URnetwork version v3.23.0\nUpdate available (v3.24.0)\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunVersionMissingVersionFile(t *testing.T) {
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, &fakePlatform{}, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "version")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.HasPrefix(err.Error(), "Cannot read the version file at ") {
		t.Errorf("err = %q", err.Error())
	}
}

func TestRunVersionMissingDateFile(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.VersionPath(), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tool, _, _ := newTestTool(install, &fakePlatform{}, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "version")
	if err == nil || err.Error() != "Cannot read the installation date file at "+install.DatePath() {
		t.Errorf("err = %v", err)
	}
}

func TestRunUpdateNoUpdateAvailable(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.VersionPath(), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(install.DatePath(), []byte("2025-01-02T03:04:05Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	releases := &fakeReleaseClient{latest: &Release{TagName: "v3.23.0", PublishedAt: time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)}}
	tool, stdout, _ := newTestTool(install, &fakePlatform{}, releases, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "update"); err != nil {
		t.Fatal(err)
	}
	want := "Installed version is up-to-date (v3.23.0)\nNo update is available\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunAutoUpdateEnableAlreadyEnabled(t *testing.T) {
	platform := &fakePlatform{autoUpdateEnabledVal: true}
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "auto-update-enable")
	if err == nil || err.Error() != "Auto update is already enabled!" {
		t.Errorf("err = %v", err)
	}
}

func TestRunAutoUpdateDisableAlreadyDisabled(t *testing.T) {
	platform := &fakePlatform{}
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "auto-update-disable")
	if err == nil || err.Error() != "Auto update is already disabled!" {
		t.Errorf("err = %v", err)
	}
}

func TestRunAutoStartEnableAlreadyEnabled(t *testing.T) {
	platform := &fakePlatform{autoStartEnabledVal: true}
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "auto-start-enable")
	if err == nil || err.Error() != "Auto-start is already enabled" {
		t.Errorf("err = %v", err)
	}
}

func TestRunAutoStartDisableNoOutput(t *testing.T) {
	platform := &fakePlatform{}
	tool, stdout, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "auto-start-disable"); err != nil {
		t.Fatal(err)
	}
	if !platform.disableAutoStartCalled {
		t.Error("auto-start-disable should call DisableAutoStart")
	}
	if stdout.String() != "" {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunAutoUpdateFreqValid(t *testing.T) {
	platform := &fakePlatform{autoUpdateEnabledVal: true}
	tool, stdout, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "every-week\n")
	if err := tool.Run(context.Background(), "auto-update-freq"); err != nil {
		t.Fatal(err)
	}
	want := "How frequently do you want the update checks to happen? every-[day/week/month]Auto update disabled\nAuto update enabled (frequency: every week)\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if platform.lastFreq != FrequencyWeek {
		t.Errorf("lastFreq = %q, want week", platform.lastFreq)
	}
}

func TestRunAutoUpdateFreqInvalid(t *testing.T) {
	platform := &fakePlatform{}
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "never\n")
	err := tool.Run(context.Background(), "auto-update-freq")
	if err == nil || err.Error() != "Invalid frequency: " {
		t.Errorf("err = %q, want %q", err, "Invalid frequency: ")
	}
}

func TestRunUnknownCommand(t *testing.T) {
	tool, _, _ := newTestTool(&Install{Path: t.TempDir()}, &fakePlatform{}, &fakeReleaseClient{}, &fakeRunner{}, "")
	err := tool.Run(context.Background(), "frobnicate")
	if err == nil || err.Error() != "Invalid command: frobnicate" {
		t.Errorf("err = %q", err)
	}
}

func TestRunCommandCaseInsensitive(t *testing.T) {
	platform := &fakePlatform{}
	tool, stdout, _ := newTestTool(&Install{Path: t.TempDir()}, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "STATUS"); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "Status: Stopped\n" {
		t.Errorf("stdout = %q", stdout.String())
	}
}

func TestRunUninstall(t *testing.T) {
	home := t.TempDir()
	old := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	defer func() { userHomeDir = old }()

	install := &Install{Path: t.TempDir()}
	for _, path := range []string{
		install.BinaryPath(),
		install.ToolsPath(),
		install.UpdaterPath(),
		install.VersionPath(),
		install.DatePath(),
	} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dataDir := install.DataDir()
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	platform := &fakePlatform{}
	tool, stdout, _ := newTestTool(install, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "uninstall"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		install.BinaryPath(),
		install.ToolsPath(),
		install.UpdaterPath(),
		install.VersionPath(),
		install.DatePath(),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s still exists", path)
		}
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Error("data dir still exists")
	}
	want := "Uninstalling URnetwork provider\nRemoving provider executable\nRemoving data directory\nRemoving startup entries (if any)\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
}

func TestRunUninstallUpdatesPath(t *testing.T) {
	home := t.TempDir()
	old := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	defer func() { userHomeDir = old }()
	install := &Install{Path: "/opt/urnetwork"}
	platform := &fakePlatform{removePathRemoved: true}
	tool, stdout, _ := newTestTool(install, platform, &fakeReleaseClient{}, &fakeRunner{}, "")
	if err := tool.Run(context.Background(), "uninstall"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Updating %PATH%\n") {
		t.Errorf("stdout missing PATH update line: %q", stdout.String())
	}
}

func TestRemovePathEntry(t *testing.T) {
	cases := []struct {
		path  string
		entry string
		sep   string
		want  string
	}{
		{path: `C:\a;C:\b;C:\a`, entry: `C:\a`, sep: ";", want: `C:\b`},
		{path: "/a:/b:/c", entry: "/b", sep: ":", want: "/a:/c"},
		{path: "/a::/c", entry: "/z", sep: ":", want: "/a:/c"},
		{path: `C:\a;;C:\b`, entry: `C:\a`, sep: ";", want: `C:\b`},
		{path: "/a:/b:/c", entry: "/B", sep: ":", want: "/a:/c"},
	}
	for _, c := range cases {
		if got := removePathEntry(c.path, c.entry, c.sep); got != c.want {
			t.Errorf("removePathEntry(%q, %q, %q) = %q, want %q", c.path, c.entry, c.sep, got, c.want)
		}
	}
}

func TestDownloadInstallerSuccess(t *testing.T) {
	content := "#!/bin/sh\necho fake\n"
	sum := sha256.Sum256([]byte(content))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(content))
	}))
	defer server.Close()
	path, err := downloadInstaller(context.Background(), server.URL, hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("downloaded content = %q, want %q", got, content)
	}
}

func TestDownloadInstallerHashMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("#!/bin/sh\necho fake\n"))
	}))
	defer server.Close()
	_, err := downloadInstaller(context.Background(), server.URL, "0000000000000000000000000000000000000000000000000000000000000000")
	if err == nil || err.Error() != "Failed to download the installer script" {
		t.Errorf("err = %v", err)
	}
}

func TestDownloadInstallerServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	_, err := downloadInstaller(context.Background(), server.URL, "0")
	if err == nil || err.Error() != "Failed to download the installer script" {
		t.Errorf("err = %v", err)
	}
}

func TestRunUpdateDownloadsAndRunsInstaller(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("windows path requires pwsh/powershell.exe; covered by the windows CI job")
	}
	content := "#!/bin/sh\necho fake\n"
	sum := sha256.Sum256([]byte(content))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(content))
	}))
	defer server.Close()

	oldURL := installerURLUnix
	oldSHA := installerSHAUnix
	installerURLUnix = server.URL
	installerSHAUnix = hex.EncodeToString(sum[:])
	defer func() {
		installerURLUnix = oldURL
		installerSHAUnix = oldSHA
	}()

	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.VersionPath(), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(install.DatePath(), []byte("2025-01-02T03:04:05Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	releases := &fakeReleaseClient{latest: &Release{TagName: "v3.24.0", PublishedAt: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)}}
	runner := &fakeRunner{}
	tool, stdout, _ := newTestTool(install, &fakePlatform{}, releases, runner, "")
	if err := tool.Run(context.Background(), "update"); err != nil {
		t.Fatal(err)
	}
	want := "Update available (v3.24.0)\nDownloading the installer script of version v3.24.0\nExecuting installer script of version v3.24.0\nUpdate completed\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner.calls = %v, want one installer run", runner.calls)
	}
	// Full-vector assertion: update must NOT append a
	// reinstall op or -Version flag. Args = sh <temp> --install <path> only.
	call := runner.calls[0]
	if len(call) != 4 {
		t.Fatalf("update args = %v, want exactly 4 (sh temp --install path)", call)
	}
	if call[0] != "sh" {
		t.Errorf("host = %q, want sh", call[0])
	}
	if call[1] == "" {
		t.Error("missing temp script path")
	}
	if call[2] != "--install" {
		t.Errorf("arg[2] = %q, want --install", call[2])
	}
	if call[3] != install.Path {
		t.Errorf("arg[3] = %q, want install path", call[3])
	}
}

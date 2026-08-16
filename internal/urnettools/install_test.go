package urnettools

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestBinarySuffixFor(t *testing.T) {
	cases := []struct {
		goos   string
		suffix string
	}{
		{goos: "windows", suffix: ".exe"},
		{goos: "linux", suffix: ""},
		{goos: "darwin", suffix: ""},
	}
	for _, c := range cases {
		if got := binarySuffixFor(c.goos); got != c.suffix {
			t.Errorf("binarySuffixFor(%q) = %q, want %q", c.goos, got, c.suffix)
		}
	}
}

func TestResolveInstallExplicitWins(t *testing.T) {
	exePath := filepath.Join(t.TempDir(), "urnet-tools")
	// Platform-neutral path: /some/explicit/dir is Unix-only and fails the
	// Abs normalization on Windows.
	explicit := filepath.Join(string(filepath.Separator), "some", "explicit", "dir")
	install := resolveInstall(explicit, exePath)
	// resolveInstall absolutizes explicit paths; on Windows Abs adds the
	// drive letter, so want must use Abs too, not Clean.
	want, err := filepath.Abs(explicit)
	if err != nil {
		t.Fatal(err)
	}
	want = filepath.Clean(want)
	if install.Path != want {
		t.Errorf("resolveInstall explicit Path = %q, want %q", install.Path, want)
	}
}

func TestResolveInstallDefaultFallsBackToExeDir(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "urnet-tools")
	install := resolveInstall("", exePath)
	if install.Path != dir {
		t.Errorf("resolveInstall default = %q, want %q", install.Path, dir)
	}
}

func TestResolveInstallDefaultUsesSignature(t *testing.T) {
	dir := t.TempDir()
	binName := "urnetwork" + binarySuffixFor(runtime.GOOS)
	if err := os.WriteFile(filepath.Join(dir, binName), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "version"), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	exePath := filepath.Join(dir, "urnet-tools"+binarySuffixFor(runtime.GOOS))
	install := resolveInstall("", exePath)
	if install.Path != dir {
		t.Errorf("resolveInstall default = %q, want %q", install.Path, dir)
	}
}

func TestResolveInstallWalksUpToSignature(t *testing.T) {
	parent := t.TempDir()
	binName := "urnetwork" + binarySuffixFor(runtime.GOOS)
	if err := os.WriteFile(filepath.Join(parent, binName), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "version"), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(parent, "child")
	if err := os.MkdirAll(child, 0o755); err != nil {
		t.Fatal(err)
	}
	exePath := filepath.Join(child, "urnet-tools")
	install := resolveInstall("", exePath)
	if install.Path != parent {
		t.Errorf("resolveInstall walked up = %q, want %q", install.Path, parent)
	}
}

func TestResolveInstallErrorPropagates(t *testing.T) {
	old := executable
	executable = func() (string, error) { return "", os.ErrNotExist }
	defer func() { executable = old }()
	if _, err := ResolveInstall(""); err == nil {
		t.Error("ResolveInstall should propagate the executable error")
	}
}

func TestInstallPaths(t *testing.T) {
	install := &Install{Path: "/opt/urnetwork"}
	suffix := binarySuffixFor(runtime.GOOS)
	join := func(name string) string { return filepath.Join("/opt/urnetwork", name+suffix) }
	cases := []struct {
		name string
		got  string
		want string
	}{
		{name: "VersionPath", got: install.VersionPath(), want: filepath.Join("/opt/urnetwork", "version")},
		{name: "DatePath", got: install.DatePath(), want: filepath.Join("/opt/urnetwork", "date")},
		{name: "BinaryPath", got: install.BinaryPath(), want: join("urnetwork")},
		{name: "ToolsPath", got: install.ToolsPath(), want: join("urnet-tools")},
		{name: "UpdaterPath", got: install.UpdaterPath(), want: join("urnetwork-updater")},
		{name: "UpdaterPIDPath", got: install.UpdaterPIDPath(), want: filepath.Join("/opt/urnetwork", "urnetwork-updater.pid")},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestDataDir(t *testing.T) {
	home := t.TempDir()
	old := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	defer func() { userHomeDir = old }()
	install := &Install{Path: "/opt/urnetwork"}
	want := filepath.Join(home, ".urnetwork")
	if got := install.DataDir(); got != want {
		t.Errorf("DataDir = %q, want %q", got, want)
	}
}

func TestReadVersion(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.VersionPath(), []byte("  v3.23.0 \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tag, err := install.ReadVersion()
	if err != nil {
		t.Fatal(err)
	}
	if tag != "v3.23.0" {
		t.Errorf("ReadVersion = %q, want v3.23.0", tag)
	}
}

func TestReadVersionEmpty(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.VersionPath(), []byte("  \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := install.ReadVersion(); err == nil {
		t.Error("ReadVersion on an empty file should error")
	}
}

func TestReadDate(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.DatePath(), []byte("2025-01-02T03:04:05Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := install.ReadDate()
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("ReadDate = %v, want %v", got, want)
	}
}

func TestReadDateInvalid(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.DatePath(), []byte("not-a-date\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := install.ReadDate(); err == nil {
		t.Error("ReadDate on garbage should error")
	}
}

func TestReadUpdaterPID(t *testing.T) {
	install := &Install{Path: t.TempDir()}
	if err := os.WriteFile(install.UpdaterPIDPath(), []byte(" 1234 \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pid, err := install.ReadUpdaterPID()
	if err != nil {
		t.Fatal(err)
	}
	if pid != 1234 {
		t.Errorf("ReadUpdaterPID = %d, want 1234", pid)
	}
}


func TestResolveInstallDotLayout(t *testing.T) {
	// A dot-layout install (Provider_Install_Linux.sh style) must be detected
	// and its accessors must point at the dot paths.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "urnetwork"), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".version"), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// ResolveInstall with the exe path inside this dir (the exe itself need
	// not exist for detection; the walk finds the signature from the exe dir).
	install := resolveInstall("", filepath.Join(dir, "urnet-tools"))
	if install.Layout != LayoutDot {
		t.Fatalf("Layout = %v, want LayoutDot", install.Layout)
	}
	if got := install.VersionPath(); got != filepath.Join(dir, ".version") {
		t.Errorf("VersionPath = %q, want %q", got, filepath.Join(dir, ".version"))
	}
	if got := install.BinaryPath(); got != filepath.Join(dir, "bin", "urnetwork") {
		t.Errorf("BinaryPath = %q, want %q", got, filepath.Join(dir, "bin", "urnetwork"))
	}
}

func TestResolveInstallFlatLayout(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "urnetwork"+binarySuffixFor(runtime.GOOS)), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "version"), []byte("v3.23.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	install := resolveInstall("", filepath.Join(dir, "urnet-tools"))
	if install.Layout != LayoutFlat {
		t.Fatalf("Layout = %v, want LayoutFlat", install.Layout)
	}
	if got := install.VersionPath(); got != filepath.Join(dir, "version") {
		t.Errorf("VersionPath = %q, want %q", got, filepath.Join(dir, "version"))
	}
}

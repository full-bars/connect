package urnettools

import (
	"strings"
	"testing"
)

func TestRenderSystemdService(t *testing.T) {
	want := `[Unit]
Description=URnetwork Provider

[Service]
ExecStart=/opt/urnetwork/urnetwork provide
Restart=no

[Install]
WantedBy=default.target
`
	if got := renderSystemdService("/opt/urnetwork/urnetwork"); got != want {
		t.Errorf("renderSystemdService:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderSystemdUpdateService(t *testing.T) {
	want := `[Unit]
Description=URnetwork Update

[Service]
Type=oneshot
ExecStart=/opt/urnetwork/urnet-tools update
`
	if got := renderSystemdUpdateService("/opt/urnetwork/urnet-tools"); got != want {
		t.Errorf("renderSystemdUpdateService:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderSystemdUpdateTimer(t *testing.T) {
	want := `[Unit]
Description=Run URnetwork Update

[Timer]
OnCalendar=weekly
Persistent=true

[Install]
WantedBy=default.target
`
	if got := renderSystemdUpdateTimer("weekly"); got != want {
		t.Errorf("renderSystemdUpdateTimer:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderProviderPlist(t *testing.T) {
	want := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.urnetwork.provider</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/urnetwork/urnetwork</string>
        <string>provide</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>WorkingDirectory</key><string>/opt/urnetwork</string>
</dict>
</plist>
`
	if got := renderProviderPlist("/opt/urnetwork/urnetwork", "/opt/urnetwork"); got != want {
		t.Errorf("renderProviderPlist:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderUpdatePlist(t *testing.T) {
	want := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.urnetwork.update</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/urnetwork/urnet-tools</string>
        <string>update</string>
    </array>
    <key>StartInterval</key><integer>604800</integer>
</dict>
</plist>
`
	if got := renderUpdatePlist("/opt/urnetwork/urnet-tools", 604800); got != want {
		t.Errorf("renderUpdatePlist:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderWindowsShortcutScript(t *testing.T) {
	got := renderWindowsShortcutScript(
		`C:\Users\You\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\urnetwork.lnk`,
		`C:\Users\You\urnetwork\urnetwork.exe`,
		"provide",
		`C:\Users\You\urnetwork`,
	)
	if !strings.Contains(got, "CreateShortcut('C:\\Users\\You\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\urnetwork.lnk')") {
		t.Errorf("shortcut path missing: %s", got)
	}
	if !strings.Contains(got, "TargetPath='C:\\Users\\You\\urnetwork\\urnetwork.exe'") {
		t.Errorf("target path missing: %s", got)
	}
	if !strings.Contains(got, "Arguments='provide'") {
		t.Errorf("arguments missing: %s", got)
	}
	if !strings.Contains(got, "WorkingDirectory='C:\\Users\\You\\urnetwork'") {
		t.Errorf("working dir missing: %s", got)
	}
	if !strings.Contains(got, "WindowStyle=0") {
		t.Errorf("window style missing: %s", got)
	}
}

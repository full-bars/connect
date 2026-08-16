package urnettools

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"
)

// renderSystemdService renders the Linux auto-start unit, matching the
// shell installer's urnetwork.service (Provider_Install_Linux.sh:328-338).
func renderSystemdService(binaryPath string) string {
	return fmt.Sprintf(`[Unit]
Description=URnetwork Provider

[Service]
ExecStart=%s provide
Restart=no

[Install]
WantedBy=default.target
`, binaryPath)
}

// renderSystemdUpdateService renders the Linux auto-update oneshot unit
// (Provider_Install_Linux.sh:341-348).
func renderSystemdUpdateService(toolsPath string) string {
	return fmt.Sprintf(`[Unit]
Description=URnetwork Update

[Service]
Type=oneshot
ExecStart=%s update
`, toolsPath)
}

// renderSystemdUpdateTimer renders the Linux auto-update timer with the
// given OnCalendar value (Provider_Install_Linux.sh:351-361).
func renderSystemdUpdateTimer(onCalendar string) string {
	return fmt.Sprintf(`[Unit]
Description=Run URnetwork Update

[Timer]
OnCalendar=%s
Persistent=true

[Install]
WantedBy=default.target
`, onCalendar)
}

// xmlEscape XML-escapes a string for safe interpolation into a plist.
func xmlEscape(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

// renderProviderPlist renders the macOS auto-start LaunchAgent plist.
func renderProviderPlist(binaryPath string, installDir string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.urnetwork.provider</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>provide</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>WorkingDirectory</key><string>%s</string>
</dict>
</plist>
`, binaryPath, installDir)
}

// renderUpdatePlist renders the macOS auto-update LaunchAgent plist with
// the frequency's StartInterval.
func renderUpdatePlist(toolsPath string, intervalSeconds int) string {
	toolsPath = xmlEscape(toolsPath)
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.urnetwork.update</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>update</string>
    </array>
    <key>StartInterval</key><integer>%d</integer>
</dict>
</plist>
`, toolsPath, intervalSeconds)
}

// renderWindowsShortcutScript builds the PowerShell one-liner that creates
// a .lnk in the Startup folder, the PS1's own mechanism. The shortcut
// targets the provider/updater binary directly (not a powershell.exe
// indirection) with WindowStyle 0 (SW_HIDE), so the started process has no
// console window. Single quotes in paths are doubled for the single-quoted
// PowerShell strings.
func renderWindowsShortcutScript(shortcutPath, targetPath, arguments, workingDir string) string {
	return fmt.Sprintf(
		"$s=(New-Object -ComObject WScript.Shell).CreateShortcut('%s');$s.TargetPath='%s';$s.Arguments='%s';$s.WorkingDirectory='%s';$s.WindowStyle=0;$s.Save()",
		strings.ReplaceAll(shortcutPath, "'", "''"),
		strings.ReplaceAll(targetPath, "'", "''"),
		strings.ReplaceAll(arguments, "'", "''"),
		strings.ReplaceAll(workingDir, "'", "''"),
	)
}

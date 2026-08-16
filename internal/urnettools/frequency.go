package urnettools

import "strings"

// Frequency selects how often the auto-update mechanism checks for a new
// release. Values mirror the PS1's `every-<freq>` forms.
type Frequency string

const (
	FrequencyDay   Frequency = "day"
	FrequencyWeek  Frequency = "week"
	FrequencyMonth Frequency = "month"
)

// ParseFrequency maps the auto-update-freq prompt answer to a Frequency.
// PowerShell's switch is case-insensitive, so the Go match is too:
// day/every-day -> day; week/every-week -> week; month/every-month ->
// month; anything else is not ok.
func ParseFrequency(s string) (Frequency, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "day", "every-day":
		return FrequencyDay, true
	case "week", "every-week":
		return FrequencyWeek, true
	case "month", "every-month":
		return FrequencyMonth, true
	}
	return "", false
}

// UpdaterFrequencySeconds maps the updater's `every-*` flag value to the
// sleep interval in seconds, the port of urnetwork-updater.ps1:8-16.
func UpdaterFrequencySeconds(s string) (int, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "every-day":
		return 60 * 60 * 24, true
	case "every-week":
		return 60 * 60 * 24 * 7, true
	case "every-month":
		return 60 * 60 * 24 * 30, true
	}
	return 0, false
}

// OnCalendar maps a Frequency to the systemd timer OnCalendar value,
// matching the shell installer's unit design (daily/weekly/monthly).
func OnCalendar(f Frequency) string {
	switch f {
	case FrequencyDay:
		return "daily"
	case FrequencyWeek:
		return "weekly"
	case FrequencyMonth:
		return "monthly"
	}
	return "daily"
}

// StartInterval maps a Frequency to the launchd StartInterval seconds.
func StartInterval(f Frequency) int {
	switch f {
	case FrequencyDay:
		return 60 * 60 * 24
	case FrequencyWeek:
		return 60 * 60 * 24 * 7
	case FrequencyMonth:
		return 60 * 60 * 24 * 30
	}
	return 60 * 60 * 24
}

package urnettools

import "testing"

func TestParseFrequency(t *testing.T) {
	cases := []struct {
		in   string
		want Frequency
		ok   bool
	}{
		{in: "day", want: FrequencyDay, ok: true},
		{in: "every-day", want: FrequencyDay, ok: true},
		{in: "week", want: FrequencyWeek, ok: true},
		{in: "every-week", want: FrequencyWeek, ok: true},
		{in: "month", want: FrequencyMonth, ok: true},
		{in: "every-month", want: FrequencyMonth, ok: true},
		{in: "EVERY-DAY", want: FrequencyDay, ok: true},
		{in: "  every-week  ", want: FrequencyWeek, ok: true},
		{in: "garbage", ok: false},
		{in: "", ok: false},
	}
	for _, c := range cases {
		got, ok := ParseFrequency(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("ParseFrequency(%q) = (%q, %v), want (%q, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestUpdaterFrequencySeconds(t *testing.T) {
	cases := []struct {
		in   string
		want int
		ok   bool
	}{
		{in: "every-day", want: 60 * 60 * 24, ok: true},
		{in: "every-week", want: 60 * 60 * 24 * 7, ok: true},
		{in: "every-month", want: 60 * 60 * 24 * 30, ok: true},
		{in: "day", ok: false},
		{in: "every-year", ok: false},
	}
	for _, c := range cases {
		got, ok := UpdaterFrequencySeconds(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("UpdaterFrequencySeconds(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestOnCalendar(t *testing.T) {
	cases := []struct {
		in   Frequency
		want string
	}{
		{in: FrequencyDay, want: "daily"},
		{in: FrequencyWeek, want: "weekly"},
		{in: FrequencyMonth, want: "monthly"},
	}
	for _, c := range cases {
		if got := OnCalendar(c.in); got != c.want {
			t.Errorf("OnCalendar(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestStartInterval(t *testing.T) {
	cases := []struct {
		in   Frequency
		want int
	}{
		{in: FrequencyDay, want: 60 * 60 * 24},
		{in: FrequencyWeek, want: 60 * 60 * 24 * 7},
		{in: FrequencyMonth, want: 60 * 60 * 24 * 30},
	}
	for _, c := range cases {
		if got := StartInterval(c.in); got != c.want {
			t.Errorf("StartInterval(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

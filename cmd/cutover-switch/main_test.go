package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 14, 0, 0, 0, la) // 30 minutes before the cut below
	const cut = "TO@2026-11-15 14:30 PT"
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string // substring of stdout or stderr
	}{
		{"help", []string{"help"}, 0, "usage:"},
		{"--help", []string{"--help"}, 0, "usage:"},
		{"no args", nil, 2, "usage:"},
		{"unknown command", []string{"show", cut}, 2, "usage:"},
		{"explain extra arg", []string{"explain", cut, "x"}, 2, "usage:"},
		{"invalid switch", []string{"explain", "TO@0"}, 1, `parse cut "TO@0"`},
		{"explain dry run", []string{"explain", "FROM@0"}, 0, "dry run: FROM owns every ts"},
		{"explain local time", []string{"explain", cut}, 0, "2026-11-15T22:30:00Z  2026-11-15 14:30 PST"},
		{"check dry run", []string{"check", "FROM@0"}, 0, "ok: dry run"},
		{"check enough lead", []string{"check", cut, "--min-lead", "30m"}, 0, "is in 30m0s"},
		{"check too close", []string{"check", cut, "--min-lead", "31m"}, 1, "want at least 31m0s"},
		{"check in the past", []string{"check", "TO@2026-11-15 13:00 PT"}, 1, "1h0m0s in the past"},
		{"check uses newest cut", []string{"check", "TO@2026-11-01 09:00 PT,FROM@2026-11-15 15:00 PT"}, 0, "is in 1h0m0s"},
		{"check bad flag", []string{"check", cut, "--lead", "30m"}, 2, "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr, now)
			out := stdout.String() + stderr.String()
			if code != tt.wantCode || !strings.Contains(out, tt.wantOut) {
				t.Errorf("code %d, output:\n%s\nwant code %d and output containing %q", code, out, tt.wantCode, tt.wantOut)
			}
		})
	}
}

func TestExplainRanges(t *testing.T) {
	var stdout bytes.Buffer
	if code := run([]string{"explain", "TO@1794781800000,FROM@1794848400000"}, &stdout, &bytes.Buffer{}, time.Now().UTC()); code != 0 {
		t.Fatalf("code %d", code)
	}
	want := `TO@1794781800000    2026-11-15T22:30:00Z  2026-11-15 22:30 UTC
FROM@1794848400000  2026-11-16T17:00:00Z  2026-11-16 17:00 UTC

ts < 2026-11-15T22:30:00Z                          FROM
2026-11-15T22:30:00Z <= ts < 2026-11-16T17:00:00Z  TO
ts >= 2026-11-16T17:00:00Z                         FROM
`
	if stdout.String() != want {
		t.Errorf("got:\n%s\nwant:\n%s", stdout.String(), want)
	}
}

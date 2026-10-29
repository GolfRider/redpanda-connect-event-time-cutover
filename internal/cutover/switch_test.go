package cutover

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

const (
	winterCut = 1794781800000 // 2026-11-15 22:30 UTC = 14:30 PST
	summerCut = 1784151000000 // 2026-07-15 21:30 UTC = 14:30 PDT
)

func TestParseSwitch(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []Cut
	}{
		{"dry run", "FROM@0", []Cut{{From, 0}}},
		{"FROM@0 implied", "TO@1794781800000", []Cut{{From, 0}, {To, winterCut}}},
		{"rollback with FROM@0 implied", "TO@1794781800000,FROM@1794800000000", []Cut{{From, 0}, {To, winterCut}, {From, 1794800000000}}},
		{"cut by epoch ms", "FROM@0,TO@1794781800000", []Cut{{From, 0}, {To, winterCut}}},
		{"rollback keeps history", "FROM@0,TO@1794781800000,FROM@1794800000000", []Cut{{From, 0}, {To, winterCut}, {From, 1794800000000}}},
		{"whitespace around cuts", " FROM@0 ,\tTO@1794781800000 ", []Cut{{From, 0}, {To, winterCut}}},
		{"smallest epoch ms", "FROM@0,TO@1000000000000", []Cut{{From, 0}, {To, 1000000000000}}},
		{"largest epoch ms", "FROM@0,TO@9999999999999", []Cut{{From, 0}, {To, 9999999999999}}},
		{"earliest date in range", "FROM@0,TO@2001-09-09T01:46:40Z", []Cut{{From, 0}, {To, 1000000000000}}},
		{"latest date in range", "FROM@0,TO@2286-11-20T17:46:39.999Z", []Cut{{From, 0}, {To, 9999999999999}}},
		{"rfc3339 utc", "FROM@0,TO@2026-11-15T22:30:00Z", []Cut{{From, 0}, {To, winterCut}}},
		{"rfc3339 offset", "FROM@0,TO@2026-11-15T14:30:00-08:00", []Cut{{From, 0}, {To, winterCut}}},
		{"rfc3339 sub-ms truncated", "FROM@0,TO@2026-11-15T22:30:00.123456Z", []Cut{{From, 0}, {To, winterCut + 123}}},
		{"simple UTC", "FROM@0,TO@2026-11-15 22:30 UTC", []Cut{{From, 0}, {To, winterCut}}},
		{"simple Z", "FROM@0,TO@2026-11-15 22:30 Z", []Cut{{From, 0}, {To, winterCut}}},
		{"simple negative offset", "FROM@0,TO@2026-11-15 14:30 -08:00", []Cut{{From, 0}, {To, winterCut}}},
		{"simple half-hour offset", "FROM@0,TO@2026-11-16 04:00 +05:30", []Cut{{From, 0}, {To, winterCut}}},
		{"one-digit hour", "FROM@0,TO@2026-11-15 9:30 UTC", []Cut{{From, 0}, {To, 1794735000000}}}, // 09:30 UTC
		{"PT in winter is PST", "FROM@0,TO@2026-11-15 14:30 PT", []Cut{{From, 0}, {To, winterCut}}},
		{"PT in summer is PDT", "FROM@0,TO@2026-07-15 14:30 PT", []Cut{{From, 0}, {To, summerCut}}},
		{"MT", "FROM@0,TO@2026-11-15 15:30 MT", []Cut{{From, 0}, {To, winterCut}}},
		{"CT", "FROM@0,TO@2026-11-15 16:30 CT", []Cut{{From, 0}, {To, winterCut}}},
		{"ET", "FROM@0,TO@2026-11-15 17:30 ET", []Cut{{From, 0}, {To, winterCut}}},
		{"IANA name", "FROM@0,TO@2026-11-15 14:30 America/Los_Angeles", []Cut{{From, 0}, {To, winterCut}}},
		{"IANA name outside US", "FROM@0,TO@2026-11-16 04:00 Asia/Kolkata", []Cut{{From, 0}, {To, winterCut}}},
		{"southern hemisphere summer", "FROM@0,TO@2026-11-15 14:30 Australia/Sydney", []Cut{{From, 0}, {To, 1794713400000}}}, // 03:30 UTC
		{"just after a 30-minute gap", "FROM@0,TO@2026-10-04 02:45 Australia/Lord_Howe", []Cut{{From, 0}, {To, 1791042300000}}},
		{"GMT is UTC", "FROM@0,TO@2026-11-15 22:30 GMT", []Cut{{From, 0}, {To, winterCut}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSwitch(tt.in)
			if err != nil {
				t.Fatalf("ParseSwitch(%q) error: %v", tt.in, err)
			}
			if !slices.Equal(got.cuts, tt.want) {
				t.Errorf("ParseSwitch(%q) = %v, want %v", tt.in, got.cuts, tt.want)
			}
		})
	}
}

func TestParseSwitchRejectsBadCuts(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"empty", "", "empty"},
		{"only whitespace", "  ", "empty"},
		{"empty cut", "FROM@0,,TO@1794781800000", "empty cut"},
		{"trailing comma", "FROM@0,", "empty cut"},
		{"missing @", "FROM0", "want FROM@<time>"},
		{"lowercase keyword", "from@0", "unknown lane"},
		{"passthrough is not a lane", "FROM@0,PASSTHROUGH@1794781800000", "unknown lane"},
		{"space before @", "FROM @0", "unknown lane"},
		{"space after @", "FROM@ 0", "unknown time"},
		{"TO at 0", "TO@0", "strictly increase"},
		{"FROM after implied FROM@0", "FROM@1794781800000", "alternate"},
		{"FROM@0 twice", "FROM@0,FROM@0", "alternate"},
		{"FROM twice", "FROM@0,FROM@1794781800000", "alternate"},
		{"TO twice", "FROM@0,TO@1794781800000,TO@1794800000000", "alternate"},
		{"equal times", "FROM@0,TO@1794781800000,FROM@1794781800000", "strictly increase"},
		{"decreasing times", "FROM@0,TO@1794781800000,FROM@1794700000000", "strictly increase"},
		{"seconds not ms", "FROM@0,TO@1794781800", "13 digits"},
		{"12 digits", "FROM@0,TO@179478180000", "13 digits"},
		{"14 digits", "FROM@0,TO@17947818000000", "13 digits"},
		{"13 digits with leading zero", "FROM@0,TO@0999999999999", "outside"},
		{"padded zero", "FROM@00", "13 digits"},
		{"thirteen zeros", "FROM@0000000000000", "outside"},
		{"date at epoch zero", "FROM@1970-01-01 00:00 UTC", "outside"},
		{"negative", "FROM@0,TO@-1", "unknown time"},
		{"plus sign", "FROM@0,TO@+1794781800000", "unknown time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSwitch(tt.in)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("ParseSwitch(%q) error = %v, want it to contain %q", tt.in, err, tt.wantErr)
			}
		})
	}
}

func TestParseSwitchRejectsBadTime(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{"rfc3339 without offset", "FROM@0,TO@2026-11-15T22:30:00", "unknown time"},
		{"rfc3339 without seconds", "FROM@0,TO@2026-11-15T22:30Z", "unknown time"},
		{"simple without zone", "FROM@0,TO@2026-11-15 22:30", "unknown time"},
		{"simple with seconds", "FROM@0,TO@2026-11-15 22:30:00 UTC", "invalid time"},
		{"12-hour clock", "FROM@0,TO@2026-11-15 2:30 pm PT", "unknown time"},
		{"double space", "FROM@0,TO@2026-11-15  22:30 UTC", "unknown time"},
		{"no such date", "FROM@0,TO@2026-02-30 10:00 UTC", "invalid time"},
		{"PST abbreviation", "FROM@0,TO@2026-11-15 14:30 PST", "unknown zone"},
		{"EST loads in Go but ignores DST", "FROM@0,TO@2026-11-15 17:30 EST", "unknown zone"},
		{"Local depends on the pod", "FROM@0,TO@2026-11-15 14:30 Local", "unknown zone"},
		{"Etc zone with inverted sign", "FROM@0,TO@2026-11-15 14:30 Etc/GMT+8", "unknown zone"},
		{"lowercase alias", "FROM@0,TO@2026-11-15 14:30 pt", "unknown zone"},
		{"unknown IANA name", "FROM@0,TO@2026-11-15 14:30 Mars/Olympus", "load zone"},
		{"skipped by spring forward", "FROM@0,TO@2026-03-08 02:30 PT", "does not exist"},
		{"repeated by fall back", "FROM@0,TO@2026-11-01 01:30 PT", "occurs twice"},
		{"1 ms before range", "FROM@0,TO@2001-09-09T01:46:39.999Z", "outside"},
		{"first ms after range", "FROM@0,TO@2286-11-20T17:46:40Z", "outside"},
		{"simple form before 2001", "FROM@0,TO@1990-01-01 00:00 UTC", "outside"},
		{"simple form after 2286", "FROM@0,TO@2300-01-01 00:00 UTC", "outside"},
		{"rfc3339 lowercase t and z", "FROM@0,TO@2026-11-15t22:30:00z", "unknown time"},
		{"leap second", "FROM@0,TO@2026-12-31T23:59:60Z", "unknown time"},
		{"rfc3339 offset without colon", "FROM@0,TO@2026-11-15T14:30:00+0530", "unknown time"},
		{"simple offset without colon", "FROM@0,TO@2026-11-15 14:30 +0530", "unknown zone"},
		{"one-digit minute", "FROM@0,TO@2026-11-15 14:3 UTC", "invalid time"},
		{"tab inside", "FROM@0,TO@2026-11-15\t22:30 UTC", "unknown time"},
		{"non-breaking space inside", "FROM@0,TO@2026-11-15 22:30 UTC", "unknown time"},
		{"southern hemisphere spring forward", "FROM@0,TO@2026-10-04 02:30 Australia/Sydney", "does not exist"},
		{"southern hemisphere fall back", "FROM@0,TO@2026-04-05 02:30 Australia/Sydney", "occurs twice"},
		{"30-minute spring forward", "FROM@0,TO@2026-10-04 02:15 Australia/Lord_Howe", "does not exist"},
		{"30-minute fall back", "FROM@0,TO@2026-04-05 01:45 Australia/Lord_Howe", "occurs twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSwitch(tt.in)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("ParseSwitch(%q) error = %v, want it to contain %q", tt.in, err, tt.wantErr)
			}
		})
	}
}

func TestOwner(t *testing.T) {
	const (
		rollbackAt = 1794800000000
		cut        = "FROM@0,TO@1794781800000" // TO at winterCut
		rollback   = cut + ",FROM@1794800000000"
	)
	tests := []struct {
		name string
		in   string
		ts   int64
		want Lane
	}{
		{"dry run serves old", "FROM@0", winterCut, From},
		{"dry run at zero", "FROM@0", 0, From},
		{"before any cut", "FROM@0", -1, From},
		{"just before cut", cut, winterCut - 1, From},
		{"exactly at cut", cut, winterCut, To},
		{"long after cut", cut, 9999999999999, To},
		{"largest possible ts", cut, math.MaxInt64, To},
		{"rollback: late event before first cut stays old", rollback, winterCut - 1, From},
		{"rollback: inside new window", rollback, winterCut, To},
		{"rollback: just before rollback", rollback, rollbackAt - 1, To},
		{"rollback: exactly at rollback", rollback, rollbackAt, From},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw, err := ParseSwitch(tt.in)
			if err != nil {
				t.Fatalf("ParseSwitch(%q) error: %v", tt.in, err)
			}
			if got := sw.Owner(tt.ts); got != tt.want {
				t.Errorf("Owner(%d) = %v, want %v", tt.ts, got, tt.want)
			}
		})
	}
}

func TestZeroSwitchBehavesLikeFromAtZero(t *testing.T) {
	for _, ts := range []int64{math.MinInt64, -1, 0, winterCut, math.MaxInt64} {
		if got := (Switch{}).Owner(ts); got != From {
			t.Errorf("Switch{}.Owner(%d) = %v, want From", ts, got)
		}
	}
}

func TestOwnerFlipsExactlyAtCuts(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) // fixed seed keeps failures reproducible
	keywords := []string{"TO", "FROM"} // cuts after FROM@0 alternate
	for range 1000 {
		in, at := "FROM@0", int64(1e12)
		for i := range rng.IntN(6) {
			at += 1 + rng.Int64N(1e10)
			in += fmt.Sprintf(",%s@%d", keywords[i%2], at)
		}
		sw, err := ParseSwitch(in)
		if err != nil {
			t.Fatalf("ParseSwitch(%q) error: %v", in, err)
		}
		for i := 1; i < len(sw.cuts); i++ {
			c, prev := sw.cuts[i], sw.cuts[i-1]
			if sw.Owner(c.At) != c.Lane || sw.Owner(c.At-1) != prev.Lane {
				t.Fatalf("switch %q: ownership does not flip exactly at %d", in, c.At)
			}
		}
	}
}

func TestCuts(t *testing.T) {
	sw, err := ParseSwitch("FROM@0,TO@1794781800000,FROM@1794800000000")
	if err != nil {
		t.Fatal(err)
	}
	want := []Cut{{From, 0}, {To, 1794781800000}, {From, 1794800000000}}
	got := sw.Cuts()
	if !slices.Equal(got, want) {
		t.Fatalf("Cuts() = %v, want %v", got, want)
	}
	got[1].Lane = From // callers must not be able to change the switch
	if sw.Owner(1794781800000) != To {
		t.Error("changing the returned cuts changed the switch")
	}
}

func TestLaneString(t *testing.T) {
	if From.String() != "from" || To.String() != "to" {
		t.Errorf("got %q and %q, want \"from\" and \"to\"", From, To)
	}
}

func TestCutString(t *testing.T) {
	tests := []struct {
		cut  Cut
		want string
	}{
		{Cut{From, 0}, "FROM@0"},
		{Cut{To, winterCut}, "TO@1794781800000"},
	}
	for _, tt := range tests {
		if got := tt.cut.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}

// FuzzParseSwitch checks that no input panics and every accepted switch
// starts at FROM@0, alternates lanes and has strictly increasing times.
func FuzzParseSwitch(f *testing.F) {
	for _, s := range []string{"FROM@0", "TO@2026-11-15 14:30 PT", "TO@1794781800000,FROM@2026-11-16T09:00:00-08:00", "TO@2026-03-08 02:30 PT", "TO@2026-11-16 04:00 +05:30"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		sw, err := ParseSwitch(s)
		if err != nil {
			return
		}
		cuts := sw.Cuts()
		if cuts[0] != (Cut{From, 0}) {
			t.Fatalf("ParseSwitch(%q) starts with %v", s, cuts[0])
		}
		for i := 1; i < len(cuts); i++ {
			if cuts[i].Lane == cuts[i-1].Lane || cuts[i].At <= cuts[i-1].At {
				t.Fatalf("ParseSwitch(%q) = %v: cuts out of order", s, cuts)
			}
		}
	})
}

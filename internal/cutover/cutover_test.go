package cutover

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string // empty means valid
	}{
		{"valid", Config{Switch: "FROM@0", From: []string{"old"}, To: []string{"new"}, Passthrough: []string{"other"}}, ""},
		{"valid without passthrough", Config{Switch: "FROM@0", From: []string{"old"}, To: []string{"new"}}, ""},
		{"invalid switch", Config{Switch: "TO@0", From: []string{"old"}, To: []string{"new"}}, "switch:"},
		{"empty switch", Config{From: []string{"old"}, To: []string{"new"}}, "switch:"},
		{"no from", Config{Switch: "FROM@0", To: []string{"new"}}, "from and to"},
		{"no to", Config{Switch: "FROM@0", From: []string{"old"}}, "from and to"},
		{"source in from and to", Config{Switch: "FROM@0", From: []string{"t"}, To: []string{"t"}}, `"t" is listed more than once`},
		{"source in from and passthrough", Config{Switch: "FROM@0", From: []string{"old"}, To: []string{"new"}, Passthrough: []string{"old"}}, `"old" is listed more than once`},
		{"duplicate within one list", Config{Switch: "FROM@0", From: []string{"old", "old"}, To: []string{"new"}}, `"old" is listed more than once`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Errorf("New() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	const ts = 1794781800000 // 2026-11-15 22:30 UTC
	tests := []struct {
		name    string
		in      any
		want    int64
		wantErr string // empty means valid
	}{
		{"json number", json.Number("1794781800000"), ts, ""},
		{"int64", int64(ts), ts, ""},
		{"uint64", uint64(ts), ts, ""},
		{"whole float64", float64(ts), ts, ""},
		{"digit string", "1794781800000", ts, ""},
		{"rfc3339 utc", "2026-11-15T22:30:00Z", ts, ""},
		{"rfc3339 offset", "2026-11-15T14:30:00-08:00", ts, ""},
		{"rfc3339 fractional seconds", "2026-11-15T22:30:00.123Z", ts + 123, ""},
		{"smallest in range", int64(1e12), 1e12, ""},
		{"largest in range", int64(1e13 - 1), 1e13 - 1, ""},
		{"missing", nil, 0, "unsupported type"},
		{"bool", true, 0, "unsupported type"},
		{"fractional float", 1794781800000.5, 0, "float timestamp"},
		{"NaN", math.NaN(), 0, "float timestamp"},
		{"fractional json number", json.Number("1794781800000.5"), 0, "neither"},
		{"exponent json number", json.Number("1.7947818e12"), 0, "neither"},
		{"seconds not ms", int64(1794781800), 0, "outside"},
		{"negative", int64(-ts), 0, "outside"},
		{"first value past range", int64(1e13), 0, "outside"},
		{"uint64 overflow", uint64(math.MaxUint64), 0, "overflows"},
		{"text", "abc", 0, "neither"},
		{"empty string", "", 0, "neither"},
		{"plus sign", "+1794781800000", 0, "neither"},
		{"leading zeros", "0001794781800000", 0, "neither"},
		{"zeros only", "0000000000000", 0, "neither"},
		{"negative string", "-1794781800000", 0, "outside"},
		{"rfc3339 without offset", "2026-11-15T22:30:00", 0, "neither"},
		{"cut switch simple form is not an event ts", "2026-11-15 14:30 PT", 0, "neither"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTimestamp(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("parseTimestamp(%v) error = %v, want it to contain %q", tt.in, err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Errorf("parseTimestamp(%v) = %d, %v; want %d", tt.in, got, err, tt.want)
			}
		})
	}
}

func TestDecide(t *testing.T) {
	const cutAt = 1794781800000
	r, err := New(Config{Switch: "FROM@0,TO@1794781800000", From: []string{"old"}, To: []string{"new"}, Passthrough: []string{"other"}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		source  string
		ts      any
		want    Outcome
		wantErr string // empty means no error
	}{
		{"passthrough ignores ts", "other", nil, Passthrough, ""},
		{"unknown source", "elsewhere", int64(cutAt), UnknownSource, "not in from, to or passthrough"},
		{"empty source", "", int64(cutAt), UnknownSource, "not in from, to or passthrough"},
		{"source checked before ts", "elsewhere", nil, UnknownSource, "not in from, to or passthrough"},
		{"old before cut is served", "old", int64(cutAt - 1), Served, ""},
		{"new before cut is dropped", "new", int64(cutAt - 1), NotServed, ""},
		{"old at cut is dropped", "old", int64(cutAt), NotServed, ""},
		{"new at cut is served", "new", int64(cutAt), Served, ""},
		{"invalid ts", "old", int64(1794781800), InvalidTS, "outside"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Decide(tt.source, tt.ts)
			if got != tt.want {
				t.Errorf("Decide(%q, %v) = %v, want %v", tt.source, tt.ts, got, tt.want)
			}
			if tt.wantErr == "" && err != nil {
				t.Errorf("Decide(%q, %v) error: %v", tt.source, tt.ts, err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Errorf("Decide(%q, %v) error = %v, want it to contain %q", tt.source, tt.ts, err, tt.wantErr)
			}
		})
	}
}

func TestDecideServesExactlyOneCopy(t *testing.T) {
	r, err := New(Config{Switch: "FROM@0,TO@1794781800000,FROM@1794800000000", From: []string{"old"}, To: []string{"new"}})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2)) // fixed seed keeps failures reproducible
	for range 10000 {
		ts := 1e12 + rng.Int64N(9e12) // anywhere in the valid ts range
		oldOut, _ := r.Decide("old", ts)
		newOut, _ := r.Decide("new", ts)
		if (oldOut == Served) == (newOut == Served) {
			t.Fatalf("ts %d: old=%v new=%v, want exactly one Served", ts, oldOut, newOut)
		}
	}
}

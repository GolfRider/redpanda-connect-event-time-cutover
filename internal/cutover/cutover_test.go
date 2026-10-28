package cutover

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/internal/cutswitch"
)

func TestNew(t *testing.T) {
	from, to := []string{"old"}, []string{"new"}
	orders := Group{Name: "orders", Events: []string{"order_created"}, Switch: "FROM@0"}
	rest := Group{Name: "rest", Events: []string{"*"}, Switch: "FROM@0"}
	tests := []struct {
		name    string
		cfg     Config
		wantErr string // empty means valid
	}{
		{"simple form", Config{Switch: "FROM@0", From: from, To: to, Passthrough: []string{"other"}}, ""},
		{"simple form without passthrough", Config{Switch: "FROM@0", From: from, To: to}, ""},
		{"groups", Config{Groups: []Group{orders, rest}, From: from, To: to, HasEventType: true}, ""},
		{"lone * group needs no event_type", Config{Groups: []Group{rest}, From: from, To: to}, ""},
		{"invalid switch", Config{Switch: "TO@0", From: from, To: to}, `switch: parse cut "TO@0": first cut must be FROM@0`},
		{"invalid group switch", Config{Groups: []Group{{Name: "orders", Events: []string{"*"}, Switch: "TO@0"}}, From: from, To: to}, `group "orders": switch:`},
		{"neither switch nor cutovers", Config{From: from, To: to}, "exactly one of switch or cutovers"},
		{"both switch and cutovers", Config{Switch: "FROM@0", Groups: []Group{rest}, From: from, To: to}, "exactly one of switch or cutovers"},
		{"empty group name", Config{Groups: []Group{{Events: []string{"*"}, Switch: "FROM@0"}}, From: from, To: to}, "name is empty or duplicated"},
		{"duplicate group name", Config{Groups: []Group{orders, {Name: "orders", Events: []string{"*"}, Switch: "FROM@0"}}, From: from, To: to, HasEventType: true}, "name is empty or duplicated"},
		{"group without events", Config{Groups: []Group{{Name: "empty", Switch: "FROM@0"}}, From: from, To: to}, "has no events"},
		{"event in two groups", Config{Groups: []Group{orders, {Name: "again", Events: []string{"order_created"}, Switch: "FROM@0"}}, From: from, To: to, HasEventType: true}, `"order_created" is in more than one group`},
		{"two * groups", Config{Groups: []Group{rest, {Name: "rest2", Events: []string{"*"}, Switch: "FROM@0"}}, From: from, To: to}, `"*" is in more than one group`},
		{"event types without event_type", Config{Groups: []Group{orders, rest}, From: from, To: to}, "event_type must be set"},
		{"no from", Config{Switch: "FROM@0", To: to}, "from and to"},
		{"no to", Config{Switch: "FROM@0", From: from}, "from and to"},
		{"source in from and to", Config{Switch: "FROM@0", From: []string{"t"}, To: []string{"t"}}, `"t" is listed more than once`},
		{"source in from and passthrough", Config{Switch: "FROM@0", From: from, To: to, Passthrough: from}, `"old" is listed more than once`},
		{"duplicate within one list", Config{Switch: "FROM@0", From: []string{"old", "old"}, To: to}, `"old" is listed more than once`},
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
	r, err := New(Config{
		Groups: []Group{
			{Name: "orders", Events: []string{"order_created"}, Switch: "FROM@0,TO@1794781800000"},
			{Name: "rest", Events: []string{"*"}, Switch: "FROM@0"},
		},
		From: []string{"old"}, To: []string{"new"}, Passthrough: []string{"other"}, HasEventType: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		source    string
		ts        any
		eventType string
		want      Outcome
		wantGroup string
		wantErr   string // empty means no error
	}{
		{"passthrough ignores ts", "other", nil, "order_created", Passthrough, "", ""},
		{"unknown source", "elsewhere", int64(cutAt), "order_created", UnknownSource, "", "not in from, to or passthrough"},
		{"empty source", "", int64(cutAt), "order_created", UnknownSource, "", "not in from, to or passthrough"},
		{"source checked before ts", "elsewhere", nil, "order_created", UnknownSource, "", "not in from, to or passthrough"},
		{"invalid ts", "old", int64(1794781800), "order_created", InvalidTS, "", "outside"},
		{"old before cut is served", "old", int64(cutAt - 1), "order_created", Served, "orders", ""},
		{"new before cut is dropped", "new", int64(cutAt - 1), "order_created", NotServed, "orders", ""},
		{"old at cut is dropped", "old", int64(cutAt), "order_created", NotServed, "orders", ""},
		{"new at cut is served", "new", int64(cutAt), "order_created", Served, "orders", ""},
		{"other event type uses * group", "old", int64(cutAt), "payment", Served, "rest", ""},
		{"same ts, other group not cut yet", "new", int64(cutAt), "payment", NotServed, "rest", ""},
		{"empty event type uses * group", "old", int64(cutAt), "", Served, "rest", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, group, err := r.Decide(tt.source, tt.ts, tt.eventType)
			if got != tt.want || group != tt.wantGroup {
				t.Errorf("Decide() = %v, %q; want %v, %q", got, group, tt.want, tt.wantGroup)
			}
			if tt.wantErr == "" && err != nil {
				t.Errorf("Decide() error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Errorf("Decide() error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestDecideGroupLookup(t *testing.T) {
	simple, err := New(Config{Switch: "FROM@0", From: []string{"old"}, To: []string{"new"}})
	if err != nil {
		t.Fatal(err)
	}
	if out, group, _ := simple.Decide("old", int64(1e12), ""); out != Served || group != "default" {
		t.Errorf("simple form: Decide() = %v, %q; want Served, \"default\"", out, group)
	}

	noCatchAll, err := New(Config{
		Groups: []Group{{Name: "orders", Events: []string{"order_created"}, Switch: "FROM@0"}},
		From:   []string{"old"}, To: []string{"new"}, HasEventType: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out, _, err := noCatchAll.Decide("old", int64(1e12), "payment"); out != UnknownEventType || err == nil {
		t.Errorf("no * group: Decide() = %v, %v; want UnknownEventType with error", out, err)
	}
}

func TestDecideServesExactlyOneCopyPerGroup(t *testing.T) {
	r, err := New(Config{
		Groups: []Group{
			{Name: "orders", Events: []string{"order_created"}, Switch: "FROM@0,TO@1794781800000,FROM@1794800000000"},
			{Name: "rest", Events: []string{"*"}, Switch: "FROM@0,TO@1794790000000"},
		},
		From: []string{"old"}, To: []string{"new"}, HasEventType: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2)) // fixed seed keeps failures reproducible
	for range 10000 {
		ts := 1e12 + rng.Int64N(9e12) // anywhere in the valid ts range
		for _, eventType := range []string{"order_created", "payment"} {
			oldOut, _, _ := r.Decide("old", ts, eventType)
			newOut, _, _ := r.Decide("new", ts, eventType)
			if (oldOut == Served) == (newOut == Served) {
				t.Fatalf("ts %d, %s: old=%v new=%v, want exactly one Served", ts, eventType, oldOut, newOut)
			}
		}
	}
}

func TestGroups(t *testing.T) {
	r, err := New(Config{
		Groups: []Group{
			{Name: "orders", Events: []string{"order_created", "order_shipped"}, Switch: "FROM@0,TO@1794781800000"},
			{Name: "rest", Events: []string{"*"}, Switch: "FROM@0"},
		},
		From: []string{"old"}, To: []string{"new"}, HasEventType: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []GroupInfo{
		{"orders", []string{"order_created", "order_shipped"}, []cutswitch.Cut{{Lane: cutswitch.From}, {Lane: cutswitch.To, At: 1794781800000}}},
		{"rest", []string{"*"}, []cutswitch.Cut{{Lane: cutswitch.From}}},
	}
	if got := r.Groups(); !reflect.DeepEqual(got, want) {
		t.Errorf("Groups() = %v, want %v", got, want)
	}

	simple, err := New(Config{Switch: "FROM@0", From: []string{"old"}, To: []string{"new"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := simple.Groups(); len(got) != 1 || got[0].Name != "default" || got[0].Events[0] != "*" {
		t.Errorf("simple form Groups() = %v, want one \"default\" group for \"*\"", got)
	}
}

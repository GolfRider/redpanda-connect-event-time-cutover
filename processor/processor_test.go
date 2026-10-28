package processor

import (
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestNewProcessor(t *testing.T) {
	const common = "from: [old]\nto: [new]\ntimestamp: this.ts\n"
	tests := []struct {
		name    string
		yaml    string
		wantErr string // empty means valid
	}{
		{"simple form", "switch: FROM@0\n" + common, ""},
		{"group form", `
cutovers:
  - {name: orders, events: [order_created], switch: "FROM@0,TO@2026-11-15 14:30 PT"}
  - {name: rest, events: ["*"], switch: FROM@0}
passthrough: [other]
event_type: this.type
` + common, ""},
		{"missing timestamp", "switch: FROM@0\nfrom: [old]\nto: [new]\n", "timestamp"},
		{"empty switch from unset env var", `switch: ""` + "\n" + common, "switch is empty"},
		{"both switch and cutovers", `
switch: FROM@0
cutovers: [{name: rest, events: ["*"], switch: FROM@0}]
` + common, "exactly one of switch or cutovers"},
		{"bad bloblang", `source: 'meta("kafka_topic'` + "\nswitch: FROM@0\n" + common, "source"},
		{"invalid switch", "switch: TO@0\n" + common, "first cut must be FROM@0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := spec().ParseYAML(tt.yaml, nil)
			if err == nil { // a config error can surface while parsing or while building
				_, err = newProcessor(conf, service.MockResources())
			}
			if tt.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Errorf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

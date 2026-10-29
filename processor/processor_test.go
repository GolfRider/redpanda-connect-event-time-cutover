package processor

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
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
		{"invalid switch", "switch: TO@0\n" + common, `event_time_cutover: switch: parse cut "TO@0"`},
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

func TestProcess(t *testing.T) {
	conf, err := spec().ParseYAML(`
cutovers:
  - {name: orders, events: [order_created], switch: "FROM@0,TO@1794781800000"}
  - {name: rest, events: ["*"], switch: FROM@0}
from: [old]
to: [new]
passthrough: [other]
timestamp: this.ts
event_type: this.type
`, nil)
	if err != nil {
		t.Fatal(err)
	}
	proc, err := newProcessor(conf, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		topic   string // empty means no kafka_topic metadata
		body    string
		kept    bool   // returned in the batch; benthos itself keeps errored messages
		wantErr string // empty means no error
	}{
		{"old copy before cut is kept", "old", `{"type":"order_created","ts":1794781799999}`, true, ""},
		{"new copy before cut is dropped", "new", `{"type":"order_created","ts":1794781799999}`, false, ""},
		{"new copy at cut is kept", "new", `{"type":"order_created","ts":1794781800000}`, true, ""},
		{"same ts, other group not cut yet", "new", `{"type":"payment","ts":1794781800000}`, false, ""},
		{"passthrough needs no ts", "other", `{}`, true, ""},
		{"unknown source", "elsewhere", `{"type":"order_created","ts":1794781800000}`, false, "not in from, to or passthrough"},
		{"missing topic", "", `{"type":"order_created","ts":1794781800000}`, false, `source "" is not in from, to or passthrough`},
		{"rfc3339 ts", "new", `{"type":"order_created","ts":"2026-11-15T22:30:00Z"}`, true, ""},
		{"bad ts", "old", `{"type":"order_created","ts":"yesterday"}`, false, "neither epoch ms nor RFC 3339"},
		{"missing ts", "old", `{"type":"order_created"}`, false, "timestamp is missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := service.NewMessage([]byte(tt.body))
			if tt.topic != "" {
				msg.MetaSet("kafka_topic", tt.topic)
			}
			batch, err := proc.Process(context.Background(), msg)
			if kept := len(batch) == 1; kept != tt.kept {
				t.Errorf("kept = %v, want %v", kept, tt.kept)
			}
			if (err == nil) != (tt.wantErr == "") || !strings.Contains(fmt.Sprint(err), tt.wantErr) {
				t.Errorf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestRegister(t *testing.T) {
	if err := Register(service.NewEmptyEnvironment()); err != nil {
		t.Fatalf("Register() error: %v", err)
	}
}

func TestStartupLog(t *testing.T) {
	const common = "from: [old]\nto: [new]\ntimestamp: this.ts\n"
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"simple form", "switch: FROM@0,TO@1000000000000\n" + common,
			`event_time_cutover: one switch for all events: cuts=[FROM@0, TO@1000000000000 (2001-09-09T01:46:40Z)] owner now=to`},
		{"group form", "cutovers: [{name: orders, events: [order_created], switch: FROM@0}]\nevent_type: this.type\n" + common,
			`event_time_cutover: group \"orders\" events=[order_created]: cuts=[FROM@0] owner now=from`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, nil)))
			conf, err := spec().ParseYAML(tt.yaml, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := newProcessor(conf, service.MockResources(service.MockResourcesOptUseLogger(logger))); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(buf.String(), tt.want) {
				t.Errorf("log = %s\nwant it to contain %s", buf.String(), tt.want)
			}
		})
	}
}

func TestMetrics(t *testing.T) {
	env := service.NewEnvironment()
	if err := Register(env); err != nil {
		t.Fatal(err)
	}
	builder := env.NewStreamBuilder()
	mux := http.NewServeMux()
	builder.SetHTTPMux(mux)
	if err := builder.SetMetricsYAML("prometheus: {}"); err != nil {
		t.Fatal(err)
	}
	err := builder.AddProcessorYAML(`
event_time_cutover:
  switch: FROM@0,TO@1794781800000
  from: [old]
  to: [new]
  timestamp: this.ts
`)
	if err != nil {
		t.Fatal(err)
	}
	send, err := builder.AddProducerFunc()
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.AddConsumerFunc(func(context.Context, *service.Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	stream, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = stream.Run(context.Background()) }()
	defer func() { _ = stream.StopWithin(time.Second) }()

	for _, topic := range []string{"old", "new", "elsewhere"} {
		msg := service.NewMessage([]byte(`{"ts":1794781799999}`))
		msg.MetaSet("kafka_topic", topic)
		if err := send(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`cutover_routed{event_type="unknown",group="default",label="",path="root.pipeline.processors.0",served="true",source="old"} 1`,
		`cutover_routed{event_type="unknown",group="default",label="",path="root.pipeline.processors.0",served="false",source="new"} 1`,
		`cutover_errors{label="",path="root.pipeline.processors.0",reason="unknown_source"} 1`,
		`cutover_cut_info{cut_ts="1794781800000",group="default",label="",owner="to",path="root.pipeline.processors.0"} 1`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics missing %s\ngot:\n%s", want, rec.Body.String())
		}
	}
}

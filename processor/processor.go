// Package processor provides the event_time_cutover processor for Redpanda Connect.
package processor

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/internal/cutover"
)

const (
	fieldSwitch      = "switch"
	fieldCutovers    = "cutovers"
	fieldName        = "name"
	fieldEvents      = "events"
	fieldFrom        = "from"
	fieldTo          = "to"
	fieldPassthrough = "passthrough"
	fieldSource      = "source"
	fieldTimestamp   = "timestamp"
	fieldEventType   = "event_type"
)

func spec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Moves a consumer from old sources to new sources by event time, with no lost or duplicated events.").
		Fields(
			service.NewStringField(fieldSwitch).Optional().
				Description(`Cut history for all events, e.g. "FROM@0,TO@2026-11-15 14:30 PT".`),
			service.NewObjectListField(fieldCutovers,
				service.NewStringField(fieldName).Description("Group name, used in logs and metrics."),
				service.NewStringListField(fieldEvents).Description(`Event types in the group; "*" matches all others.`),
				service.NewStringField(fieldSwitch).Description("Cut history for the group."),
			).Optional().Description("Groups of event types, each with its own switch. Use instead of switch."),
			service.NewStringListField(fieldFrom).Description("Old sources."),
			service.NewStringListField(fieldTo).Description("New sources carrying the same events."),
			service.NewStringListField(fieldPassthrough).Optional().Description("Sources that are not migrating."),
			service.NewBloblangField(fieldSource).Default(`meta("kafka_topic")`).Description("Where a message came from."),
			service.NewBloblangField(fieldTimestamp).Description("Event time set by the producer: epoch ms or RFC 3339."),
			service.NewBloblangField(fieldEventType).Optional().Description("Event type; required when a group lists event types."),
		)
}

type processor struct {
	router    *cutover.Router
	source    *bloblang.Executor
	timestamp *bloblang.Executor
	eventType *bloblang.Executor // nil when event_type is not set
}

func newProcessor(conf *service.ParsedConfig, _ *service.Resources) (*processor, error) {
	cfg, err := routerConfig(conf)
	if err != nil {
		return nil, err
	}
	p := &processor{}
	if p.router, err = cutover.New(cfg); err != nil {
		return nil, err
	}
	if p.source, err = conf.FieldBloblang(fieldSource); err != nil {
		return nil, err
	}
	if p.timestamp, err = conf.FieldBloblang(fieldTimestamp); err != nil {
		return nil, err
	}
	if cfg.HasEventType {
		if p.eventType, err = conf.FieldBloblang(fieldEventType); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// routerConfig reads the fields that cutover.New validates.
func routerConfig(conf *service.ParsedConfig) (cutover.Config, error) {
	cfg := cutover.Config{HasEventType: conf.Contains(fieldEventType)}
	var err error
	if cfg.From, err = conf.FieldStringList(fieldFrom); err != nil {
		return cutover.Config{}, err
	}
	if cfg.To, err = conf.FieldStringList(fieldTo); err != nil {
		return cutover.Config{}, err
	}
	if conf.Contains(fieldPassthrough) {
		if cfg.Passthrough, err = conf.FieldStringList(fieldPassthrough); err != nil {
			return cutover.Config{}, err
		}
	}
	if conf.Contains(fieldSwitch) {
		if cfg.Switch, err = conf.FieldString(fieldSwitch); err != nil {
			return cutover.Config{}, err
		}
		// An unset ${VAR} becomes "", which would otherwise read as "no switch given".
		if cfg.Switch == "" {
			return cutover.Config{}, errors.New("switch is empty; is its environment variable set?")
		}
	}
	if conf.Contains(fieldCutovers) {
		if cfg.Groups, err = readGroups(conf); err != nil {
			return cutover.Config{}, err
		}
	}
	return cfg, nil
}

func readGroups(conf *service.ParsedConfig) ([]cutover.Group, error) {
	list, err := conf.FieldObjectList(fieldCutovers)
	if err != nil {
		return nil, err
	}
	groups := make([]cutover.Group, 0, len(list))
	for _, item := range list {
		var g cutover.Group
		if g.Name, err = item.FieldString(fieldName); err != nil {
			return nil, err
		}
		if g.Events, err = item.FieldStringList(fieldEvents); err != nil {
			return nil, err
		}
		if g.Switch, err = item.FieldString(fieldSwitch); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, nil
}

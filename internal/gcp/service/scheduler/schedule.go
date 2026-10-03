package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	_ "time/tzdata" // embed the IANA tz database so named time zones resolve anywhere

	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// ParseSchedule parses a unix-cron schedule (5-field) or an @descriptor in the
// given IANA time zone. An empty timeZone defaults to UTC. English-like
// schedules ("every 5 minutes") are not parsed and fail loud.
func ParseSchedule(expr, tz string) (cron.Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, invalidArgument("schedule is required")
	}
	if tz == "" {
		tz = "UTC"
	}
	if strings.EqualFold(tz, "utc") {
		tz = "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return nil, invalidArgument(fmt.Sprintf("invalid timeZone %q", tz))
	}
	sched, err := cron.ParseStandard("CRON_TZ=" + tz + " " + expr)
	if err != nil {
		return nil, invalidArgument(fmt.Sprintf("invalid schedule %q: %v", expr, err))
	}
	return sched, nil
}

// nextFireFor computes the next execution time for a job's schedule, in UTC.
// On a parse failure it falls back to an hour ahead so the engine keeps making
// progress rather than spinning (validation rejects bad schedules at write time).
func nextFireFor(j schedstore.Job, now time.Time) time.Time {
	sched, err := ParseSchedule(j.Schedule, j.TimeZone)
	if err != nil {
		return now.Add(time.Hour)
	}
	return sched.Next(now.UTC())
}

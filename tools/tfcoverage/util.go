package main

import (
	"fmt"
	"time"
)

const dateLayout = "2006-01-02"

func parseDate(value string) (time.Time, error) {
	d, err := time.Parse(dateLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not YYYY-MM-DD: %w", value, err)
	}
	return d, nil
}

//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/datastore"
)

// parityTask is the entity shape used by the datastore probe. The field names
// are kept stable across runs; only the key name carries the per-run suffix.
type parityTask struct {
	Description string
	Priority    int
}

// seedDatastore writes one entity through the official datastore client, then
// returns checks that read it back after a restart and assert its absence after
// a reset.
func seedDatastore(d *driver, suffix string) (func() error, func() error, func() error, error) {
	key := datastore.NameKey("ParityTask", "task-"+suffix, nil)
	const seededDescription = "seeded"
	const seededPriority = 7

	write := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := d.datastoreClient(ctx)
		if err != nil {
			return fmt.Errorf("datastore client: %w", err)
		}
		defer c.Close()
		if _, err := c.Put(ctx, key, &parityTask{Description: seededDescription, Priority: seededPriority}); err != nil {
			return fmt.Errorf("datastore put: %w", err)
		}
		return nil
	}

	// get returns the entity and a nil error only when it is present.
	get := func() (parityTask, error) {
		var got parityTask
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := d.datastoreClient(ctx)
		if err != nil {
			return got, fmt.Errorf("datastore client: %w", err)
		}
		defer c.Close()
		if err := c.Get(ctx, key, &got); err != nil {
			return got, err
		}
		return got, nil
	}

	seeded := func() error {
		got, err := get()
		if err != nil {
			return fmt.Errorf("datastore get after seed: %w", err)
		}
		if got.Description != seededDescription || got.Priority != seededPriority {
			return fmt.Errorf("datastore entity mismatch after seed: %+v", got)
		}
		return nil
	}
	survived := func() error {
		got, err := get()
		if err != nil {
			return fmt.Errorf("datastore get after restart: %w", err)
		}
		if got.Description != seededDescription || got.Priority != seededPriority {
			return fmt.Errorf("datastore entity mismatch after restart: %+v", got)
		}
		return nil
	}
	cleared := func() error {
		got, err := get()
		if err == nil {
			return fmt.Errorf("datastore entity still present after reset: %+v", got)
		}
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			return fmt.Errorf("datastore get after reset: got %v, want ErrNoSuchEntity", err)
		}
		return nil
	}

	if err := write(); err != nil {
		return nil, nil, nil, err
	}
	// Confirm the seed is visible before the restart, so a later "survived"
	// failure can only mean the row was lost, never that the write never landed.
	if err := seeded(); err != nil {
		return nil, nil, nil, err
	}
	return seeded, survived, cleared, nil
}

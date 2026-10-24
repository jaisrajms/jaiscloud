package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// newFirestoreClient uses the FIRESTORE_EMULATOR_HOST hook (set in main).
func newFirestoreClient(ctx context.Context, cfg Config) (*firestore.Client, error) {
	return firestore.NewClient(ctx, cfg.Project, option.WithoutAuthentication())
}

func firestoreScenarios(r *runner, f *fixtures) {
	r.run("firestore.listen_write", "OK", func(ctx context.Context) (string, string, error) {
		c, err := newFirestoreClient(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		col := c.Collection(rid(r.cfg, "fs-listen"))
		it := col.Query.Snapshots(ctx)
		defer it.Stop()
		// Drain the initial CURRENT snapshot.
		if _, err := it.Next(); err != nil {
			return "", "", fmt.Errorf("initial snapshot: %w", err)
		}
		// A listen is a long-lived server stream; write a doc and wait for the
		// ADD frame that carries it.
		if _, err := col.Doc("live-doc").Set(ctx, map[string]any{"v": 1}); err != nil {
			return "", "", fmt.Errorf("write: %w", err)
		}
		for {
			snap, err := it.Next()
			if err != nil {
				return "", "", fmt.Errorf("listen next: %w", err)
			}
			docs := snap.Documents
			for {
				d, err := docs.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return "", "", fmt.Errorf("listen docs: %w", err)
				}
				if d.Ref.ID == "live-doc" {
					return "true", "Listen delivered ADD for live-doc", nil
				}
			}
		}
	})

	r.run("firestore.transaction", "OK", func(ctx context.Context) (string, string, error) {
		c, err := newFirestoreClient(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		col := c.Collection(rid(r.cfg, "fs-tx"))
		ref := col.Doc("counter")
		if _, err := ref.Set(ctx, map[string]any{"n": 0}); err != nil {
			return "", "", err
		}
		for i := 0; i < 3; i++ {
			err = c.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
				snap, err := tx.Get(ref)
				if err != nil {
					return err
				}
				var d struct {
					N int `firestore:"n"`
				}
				if err := snap.DataTo(&d); err != nil {
					return err
				}
				return tx.Set(ref, map[string]any{"n": d.N + 1})
			})
			if err != nil {
				return "", "", fmt.Errorf("transaction %d: %w", i, err)
			}
		}
		snap, err := ref.Get(ctx)
		if err != nil {
			return "", "", err
		}
		var d struct {
			N int `firestore:"n"`
		}
		if err := snap.DataTo(&d); err != nil {
			return "", "", err
		}
		if d.N != 3 {
			return "", "", fmt.Errorf("counter = %d, want 3", d.N)
		}
		return fmt.Sprintf("%d", d.N), "3 read-modify-write transactions", nil
	})

	r.run("firestore.query_pagination", "OK", func(ctx context.Context) (string, string, error) {
		c, err := newFirestoreClient(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		col := c.Collection(rid(r.cfg, "fs-page"))
		for i := 0; i < 5; i++ {
			if _, err := col.Doc(fmt.Sprintf("d%d", i)).Set(ctx, map[string]any{"i": i}); err != nil {
				return "", "", err
			}
		}
		query := col.Query.OrderBy("i", firestore.Asc)
		var cursor *firestore.DocumentSnapshot
		pages, total := 0, 0
		for {
			q := query
			if cursor != nil {
				q = query.StartAfter(cursor)
			}
			it := q.Limit(2).Documents(ctx)
			n := 0
			for {
				doc, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return "", "", fmt.Errorf("query page: %w", err)
				}
				cursor = doc
				n++
			}
			if n == 0 {
				break
			}
			pages++
			total += n
		}
		if total != 5 {
			return "", "", fmt.Errorf("paged %d docs, want 5", total)
		}
		return fmt.Sprintf("pages=%d,total=%d", pages, total), fmt.Sprintf("limit=2 pages=%d total=%d", pages, total), nil
	})
}

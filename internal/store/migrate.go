package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var SharedMigrationFS embed.FS

// RunMigrations creates the cloud schema if it does not exist, then applies
// all unapplied SQL migration files from migrationsFS in sorted order. source
// identifies the caller ("shared", "aws", "azure") and is stored in
// jc_schema_migrations to scope the applied-count check. Safe to call on
// every startup — already-applied files are skipped.
//
// cloud must be an allowlist-validated value ("aws", "azure", "gcp").
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, cloud string, migrationsFS embed.FS, source string) error {
	// Serialize concurrent migrations with a session advisory lock held on a
	// dedicated connection. Two processes starting against the same database
	// (e.g. parallel integration-test packages) would otherwise race on the
	// jc_schema_migrations bookkeeping table.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(0x6a616973)); err != nil {
		return fmt.Errorf("migration advisory lock: %w", err)
	}
	defer conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", int64(0x6a616973))

	schemaIdent := pgx.Identifier{cloud}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaIdent); err != nil {
		return fmt.Errorf("create schema %s: %w", cloud, err)
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jc_schema_migrations (
			filename   TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			checksum   TEXT        NOT NULL DEFAULT ''
		)
	`)
	if err != nil {
		return fmt.Errorf("create jc_schema_migrations: %w", err)
	}

	// Add source column idempotently — existing rows default to 'shared'.
	if _, err := pool.Exec(ctx, `
		ALTER TABLE jc_schema_migrations
		ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'shared'
	`); err != nil {
		return fmt.Errorf("add source column: %w", err)
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jc_meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("create jc_meta: %w", err)
	}

	// Collect SQL file names in sorted order.
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		data, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		sum := checksumSQL(data)

		var storedSum string
		var count int
		err = pool.QueryRow(ctx,
			`SELECT count(*), coalesce(max(checksum),'') FROM jc_schema_migrations WHERE filename=$1`, name,
		).Scan(&count, &storedSum)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if count > 0 {
			if storedSum != "" && storedSum != sum {
				return fmt.Errorf("migration %s checksum mismatch: stored=%s computed=%s; "+
					"migration files must not be modified after deployment", name, storedSum, sum)
			}
			continue
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO jc_schema_migrations (filename, checksum, source) VALUES ($1, $2, $3)`, name, sum, source,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}

	// Scope the count check to this source only.
	var appliedCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM jc_schema_migrations WHERE source = $1`, source,
	).Scan(&appliedCount); err != nil {
		return fmt.Errorf("count applied migrations: %w", err)
	}
	expectedCount := len(names)
	if appliedCount > expectedCount {
		return fmt.Errorf("source=%q: applied migration count (%d) is ahead of this binary (%d); "+
			"upgrade jaiscloud to the latest release", source, appliedCount, expectedCount)
	}
	if appliedCount < expectedCount {
		return fmt.Errorf("source=%q: applied migration count (%d) is behind this binary (%d); "+
			"run with --fresh-start to wipe and reinitialize the database", source, appliedCount, expectedCount)
	}

	// Cloud identity guard: ensure the DB was initialized for the same cloud.
	var storedCloud string
	err = pool.QueryRow(ctx, `SELECT value FROM jc_meta WHERE key='cloud'`).Scan(&storedCloud)
	if err != nil {
		if _, err := pool.Exec(ctx,
			`INSERT INTO jc_meta (key, value) VALUES ('cloud', $1) ON CONFLICT (key) DO NOTHING`, cloud,
		); err != nil {
			return fmt.Errorf("write cloud identity: %w", err)
		}
	} else if storedCloud != cloud {
		return fmt.Errorf("database cloud mismatch: stored=%q running=%q; "+
			"use jaiscloud-%s with this database or run with --fresh-start to reinitialize", storedCloud, cloud, storedCloud)
	}

	return nil
}

func checksumSQL(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Package gcpstore holds the GCP-specific Postgres migrations. GCP follows the
// same per-cloud schema model as AWS: the shared ResourceStore migration runs
// into the "gcp" search_path schema, and the GCP-specific tables below run
// into that same schema via their own MigrationFS.
package gcpstore

import "embed"

//go:embed migrations/*.sql
var MigrationFS embed.FS

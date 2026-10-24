package resourcemanager

import (
	"testing"
	"time"

	core "jaiscloud/internal/gcp/service/resourcemanager"
)

// TestProjectToV1JSONTimestampSpelling pins the protojson spelling of the
// Project createTime google-datetime field.
func TestProjectToV1JSONTimestampSpelling(t *testing.T) {
	out := projectToV1JSON(core.Project{
		ProjectID:  "proj",
		CreateTime: time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
	})
	if got := out["createTime"]; got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("createTime = %v, want 2024-03-01T12:00:00.500Z", got)
	}
}

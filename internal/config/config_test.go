package config_test

import (
	"testing"

	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

func TestConfig_DefaultIsNotEphemeral(t *testing.T) {
	t.Setenv("JAISCLOUD_EPHEMERAL", "")
	t.Setenv("JAISCLOUD_DSN", "")

	cfg, err := config.Load(model.CloudAWS)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Ephemeral {
		t.Error("expected Ephemeral=false by default")
	}
}

func TestConfig_EphemeralFromEnv(t *testing.T) {
	t.Setenv("JAISCLOUD_EPHEMERAL", "true")
	t.Setenv("JAISCLOUD_DSN", "")

	cfg, err := config.Load(model.CloudAWS)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Ephemeral {
		t.Error("expected Ephemeral=true when JAISCLOUD_EPHEMERAL=true")
	}
}

func TestConfig_EphemeralAndDSNRejected(t *testing.T) {
	t.Setenv("JAISCLOUD_EPHEMERAL", "true")
	t.Setenv("JAISCLOUD_DSN", "postgres://localhost/test")

	_, err := config.Load(model.CloudAWS)
	if err == nil {
		t.Error("expected error when both JAISCLOUD_EPHEMERAL and JAISCLOUD_DSN are set")
	}
}

func TestConfig_CloudSpecificIsolation(t *testing.T) {
	// GCP load must not populate AWS-specific fields.
	gcp, err := config.Load(model.CloudGCP)
	if err != nil {
		t.Fatalf("Load(gcp): %v", err)
	}
	if gcp.ProjectID == "" {
		t.Error("expected GCP ProjectID to be set")
	}
	if gcp.IMDSEnabled || gcp.S3VirtualHostBases != nil || gcp.AWSEmulatorEndpoint != "" {
		t.Errorf("GCP load must not set AWS-specific config: imds=%v s3vhb=%v aws_ep=%q",
			gcp.IMDSEnabled, gcp.S3VirtualHostBases, gcp.AWSEmulatorEndpoint)
	}

	// AWS load must not populate GCP-specific fields.
	aws, err := config.Load(model.CloudAWS)
	if err != nil {
		t.Fatalf("Load(aws): %v", err)
	}
	if aws.GCPServiceAccount != "" || aws.GCPMetadataEnabled || aws.ProjectID != "" {
		t.Errorf("AWS load must not set GCP-specific config: sa=%q metadata=%v project=%q",
			aws.GCPServiceAccount, aws.GCPMetadataEnabled, aws.ProjectID)
	}
}

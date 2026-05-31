package ui

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/aws/key"
	"jaiscloud/internal/aws/parameter"
	"jaiscloud/internal/aws/provider/apigw"
	"jaiscloud/internal/aws/provider/catalog"
	"jaiscloud/internal/aws/provider/cloudwatch"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/aws/provider/emr"
	"jaiscloud/internal/aws/provider/emroneks"
	"jaiscloud/internal/aws/provider/events"
	"jaiscloud/internal/aws/provider/iam"
	"jaiscloud/internal/aws/provider/lambda"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/aws/provider/stepfunctions"
	"jaiscloud/internal/aws/provider/table"
	"jaiscloud/internal/aws/secret"
	"jaiscloud/internal/aws/ui/sse"
	"jaiscloud/internal/config"
	internalevents "jaiscloud/internal/events"
)

// AWSProviders holds all provider pointers injected from main.go.
// A nil provider means the service is not wired; its nav entry is hidden.
type AWSProviders struct {
	Queue    *queue.QueueProvider
	Object   *object.ObjectProvider
	Table    *table.TableProvider
	Function *lambda.FunctionProvider
	Logs     *cwlogs.Provider
	CW       *cloudwatch.Provider
	Notif    *notification.SNSProvider
	IAM      *iam.IAMProvider
	Key      *key.KeyProvider
	Secret   *secret.SecretProvider
	Param    *parameter.ParameterProvider
	APIGW    *apigw.GatewayProvider
	Catalog  *catalog.GlueProvider
	EMR      *emr.EMRProvider
	EMRC     *emroneks.EMRContainersProvider
	Events   *events.EventBridgeProvider
	Sfn      *stepfunctions.Provider
}

// UIServer is the lightweight HTTP server for the UI (port 4567).
// Entirely separate from gateway.Server (port 4566).
type UIServer struct {
	srv     *http.Server
	broker  *sse.Broker
	token   string
	version string
}

// New creates a UIServer. Returns (nil, nil) if Assets() == nil (binary built without -tags ui).
// Generates a 32-byte session token and writes it atomically to the token file.
// version is the binary version string (e.g. "dev", "v1.2.3").
func New(providers *AWSProviders, adminHandler *admin.Handler, cfg *config.Config, bus *internalevents.EventBus, version string) (*UIServer, error) {
	assets, err := StaticFS()
	if err != nil {
		return nil, fmt.Errorf("ui: load static assets: %w", err)
	}
	if assets == nil {
		return nil, nil
	}

	tp := tokenPath(cfg)
	if err := os.MkdirAll(filepath.Dir(tp), 0700); err != nil {
		return nil, fmt.Errorf("ui: create token dir: %w", err)
	}
	token, err := writeToken(tp)
	if err != nil {
		return nil, fmt.Errorf("ui: write session token: %w", err)
	}

	broker := sse.New(bus)

	router := BuildRouter(providers, adminHandler, broker, cfg, token, version)

	s := &UIServer{
		srv: &http.Server{
			Handler: router,
		},
		broker:  broker,
		token:   token,
		version: version,
	}
	return s, nil
}

// ListenAndServe starts the UI HTTP listener on addr (e.g. ":4567").
func (s *UIServer) ListenAndServe(addr string) error {
	s.srv.Addr = addr
	return s.srv.ListenAndServe()
}

// Shutdown closes the SSE broker then gracefully shuts down the HTTP server.
func (s *UIServer) Shutdown(ctx context.Context) error {
	s.broker.Shutdown()
	return s.srv.Shutdown(ctx)
}

// tokenPath returns the file path for the session token.
func tokenPath(cfg *config.Config) string {
	if cfg.DataDir != "" {
		return filepath.Join(cfg.DataDir, ".session-token")
	}
	base, _ := os.UserConfigDir()
	return filepath.Join(base, "jaiscloud-ui", ".session-token")
}

// writeToken generates a 32-byte hex token and writes it atomically (mode 0600).
func writeToken(path string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := hex.EncodeToString(b)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(token), 0600); err != nil {
		return "", err
	}
	return token, os.Rename(tmp, path)
}

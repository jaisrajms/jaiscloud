package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gateway"
	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/crypto"
	grpcserver "jaiscloud/internal/gcp/grpc"
	grpcfirestore "jaiscloud/internal/gcp/grpc/firestore"
	grpckms "jaiscloud/internal/gcp/grpc/kms"
	grpcpubsub "jaiscloud/internal/gcp/grpc/pubsub"
	grpcsecretmanager "jaiscloud/internal/gcp/grpc/secretmanager"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	iamprovider "jaiscloud/internal/gcp/provider/iam"
	kmsprovider "jaiscloud/internal/gcp/provider/kms"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	secretmanagerprovider "jaiscloud/internal/gcp/provider/secretmanager"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	gcpstore "jaiscloud/internal/gcp/store"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/store/gcs"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/persistence/snapshot"
	snapversion "jaiscloud/internal/persistence/version"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/snapshottypes"
	"jaiscloud/internal/store"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const version = "0.2.0"

const defaultHost = "http://localhost:8080"

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud-gcp",
		Short: "JaisCloud GCP - local GCP emulator",
	}
	root.AddCommand(startCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(envCmd())
	root.AddCommand(doctorCmd())
	root.AddCommand(resetCmd())
	root.AddCommand(exportCmd())
	root.AddCommand(importCmd())
	root.AddCommand(snapshotCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the emulator",
		RunE: func(cmd *cobra.Command, args []string) error {
			bindFlags(cmd)

			cfg, err := config.Load(model.CloudGCP)
			if err != nil {
				return err
			}
			// GCP has no account ID; map the project onto the account scope used
			// by the shared store. Region defaults to global (GCP services omit
			// region from most REST paths).
			cfg.AccountID = cfg.ProjectID
			cfg.Region = "global"
			// config.Load defaults the port to 4566 (the AWS default); GCP listens
			// on 8080 unless the user overrides via --port or JAISCLOUD_PORT.
			if !cmd.Flags().Changed("port") && os.Getenv("JAISCLOUD_PORT") == "" {
				cfg.Port = 8080
			}
			clock.SetGlobalClock(cfg.Clock)

			ctx := context.Background()

			stateDir, _ := config.ResolveStateDir(os.Getenv("JAISCLOUD_STATE_DIR"))
			instanceID, _ := config.LoadOrCreateInstanceID(stateDir)

			stores, err := initStores(ctx, cfg, instanceID)
			if err != nil {
				return err
			}
			defer stores.close()

			// KMS envelope encryption: when a master key (KEK) is configured,
			// wrap the server DEK at rest (mirrors AWS key bootstrap.LoadOrCreateDEK).
			if cfg.KMSMasterKey != "" {
				kek, err := kmsstore.ParseHexKey(cfg.KMSMasterKey)
				if err != nil {
					return fmt.Errorf("kms master key: %w", err)
				}
				if ps, ok := stores.keys.(*kmsstore.PostgresStore); ok {
					ps.SetKEK(kek)
				}
			}

			storageP := storageprovider.New(stores.objects, stores.resources, stores.blobs, crypto.NewEnvelopeEncryptor(stores.keys))
			secretP := secretmanagerprovider.New(stores.secrets, stores.resources, crypto.NewEnvelopeEncryptor(stores.keys))
			kmsP := kmsprovider.New(stores.keys)
			iamP := iamprovider.New(stores.resources)
			pubsubP := pubsubprovider.New(stores.resources, stores.messages, crypto.NewEnvelopeEncryptor(stores.keys))
			firestoreP := firestoreprovider.New(stores.documents, stores.resources)

			reg := provider.NewRegistry().
				Register(storageP).
				Register(secretP).
				Register(kmsP).
				Register(iamP).
				Register(pubsubP).
				Register(firestoreP)

			// gRPC transport shares the SAME Firestore provider Service as the
			// REST adapter, so both transports use one transaction read-set
			// registry.
			grpcPort, _ := cmd.Flags().GetInt("grpc-port")
			firestoreGRPC := grpcfirestore.NewService(firestoreP.Service, cfg.ProjectID)
			pubsubGRPC := grpcpubsub.NewService(stores.resources, stores.messages, crypto.NewEnvelopeEncryptor(stores.keys), cfg.ProjectID)
			secretGRPC := grpcsecretmanager.NewService(stores.secrets, stores.resources, crypto.NewEnvelopeEncryptor(stores.keys), cfg.ProjectID)
			kmsGRPC := grpckms.NewService(stores.keys, stores.resources, crypto.NewEnvelopeEncryptor(stores.keys), cfg.ProjectID)
			gserv := grpcserver.NewServer(fmt.Sprintf(":%d", grpcPort))
			firestorepb.RegisterFirestoreServer(gserv.GRPC(), firestoreGRPC)
			pubsubpb.RegisterPublisherServer(gserv.GRPC(), pubsubGRPC)
			pubsubpb.RegisterSubscriberServer(gserv.GRPC(), pubsubGRPC)
			kmspb.RegisterKeyManagementServiceServer(gserv.GRPC(), kmsGRPC)
			// Secret Manager's IAM surface (GetIamPolicy/SetIamPolicy/
			// TestIamPermissions) is served by the SecretManagerService itself
			// (its proto embeds the methods), so it does not re-register the
			// standalone google.iam.v1.IAMPolicy service that Pub/Sub and KMS own.
			// Pub/Sub and KMS share the single IAMPolicy service, so their IAM
			// surfaces are dispatched through one router.
			secretmanagerpb.RegisterSecretManagerServiceServer(gserv.GRPC(), secretGRPC)
			iampb.RegisterIAMPolicyServer(gserv.GRPC(), grpcserver.NewIAMRouter(pubsubGRPC, kmsGRPC))

			adminHandler := admin.NewHandler()
			adminHandler.RegisterResetter(stores.objects)
			adminHandler.RegisterResetter(stores.messages)
			adminHandler.RegisterResetter(stores.secrets)
			adminHandler.RegisterResetter(stores.keys)
			adminHandler.RegisterResetter(stores.documents)
			adminHandler.RegisterResetter(stores.resources)
			adminHandler.RegisterResetter(stores.blobs)
			adminHandler.RegisterResetter(storageP)
			adminHandler.RegisterResetter(firestoreP)
			adminHandler.RegisterResetter(firestoreGRPC)
			if snap, ok := stores.resources.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("resources", snap)
			}
			if snap, ok := stores.objects.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("gcs_objects", snap)
			}
			if snap, ok := stores.messages.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("pubsub_messages", snap)
			}
			if snap, ok := stores.secrets.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("secrets", snap)
			}
			if snap, ok := stores.keys.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("keys", snap)
			}
			if snap, ok := stores.documents.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("firestore_documents", snap)
			}
			if sb, ok := stores.blobs.(admin.SnapshotBlobStore); ok {
				adminHandler.RegisterBlobStore(sb)
			}
			adminHandler.SetMeta(admin.HandlerMeta{
				Cloud:     "gcp",
				Region:    cfg.Region,
				AccountID: cfg.ProjectID,
				StateDir:  stateDir,
			})
			if cfg.KMSMasterKey != "" {
				adminHandler.SetKEKFingerprint(snapversion.FingerprintKEK([]byte(cfg.KMSMasterKey)))
			}

			cloudAdapter := gcpadapter.NewAdapter(cfg.GCPServiceAccount)

			var certs certstore.CertStore
			if fsCS, err := certstore.NewFilesystemCertStore(stateDir); err == nil {
				certs = fsCS
			} else {
				certs = certstore.NewMemoryCertStore()
			}

			barrier := snapshot.NewBarrier()
			adminHandler.SetBarrier(barrier)

			var gatewayOpts []func(*gateway.Server)
			gatewayOpts = append(gatewayOpts, gateway.WithBarrier(barrier))
			if cfg.GCPMetadataEnabled {
				metaCfg := gcpadapter.MetadataConfig{
					ProjectID:      cfg.ProjectID,
					ServiceAccount: cfg.GCPServiceAccount,
				}
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					gcpadapter.RegisterMetadataRoutes(r, metaCfg)
				}))
			}

			dataDir := cfg.DataDir
			if dataDir == "" {
				if home, err := os.UserHomeDir(); err == nil {
					dataDir = filepath.Join(home, ".jaiscloud", "jaiscloud-gcp")
				} else {
					dataDir = filepath.Join(".jaiscloud", "jaiscloud-gcp")
				}
			}
			adminHandler.SetDataDir(dataDir)

			var cleanup func() = func() {}
			if !cfg.Ephemeral && cfg.DSN == "" {
				adminSnaps := adminHandler.Snapshotters()
				loopStores := make(map[string]snapshottypes.Snapshotter, len(adminSnaps))
				for k, v := range adminSnaps {
					loopStores[k] = v
				}

				stateFile := filepath.Join(dataDir, "state.json")
				if stateData, readErr := os.ReadFile(stateFile); readErr == nil {
					var env snapversion.Envelope
					if parseErr := json.Unmarshal(stateData, &env); parseErr != nil {
						slog.Warn("startup: state.json parse failed; starting fresh", "err", parseErr)
					} else if versionErr := snapversion.CheckSnapshotVersion(env.SchemaVersion); versionErr != nil {
						return fmt.Errorf("startup: state.json version check failed: %w\nRun with --fresh-start to wipe state", versionErr)
					} else if env.Cloud != "" && env.Cloud != string(cfg.Cloud) {
						return fmt.Errorf("startup: state.json cloud mismatch: stored=%q running=%q", env.Cloud, cfg.Cloud)
					} else {
						for name, s := range loopStores {
							data, ok := env.Stores[name]
							if !ok {
								continue
							}
							if restoreErr := s.Restore(ctx, bytes.NewReader(data)); restoreErr != nil {
								slog.Warn("startup: restore store failed; skipping", "store", name, "err", restoreErr)
							}
						}
						slog.Info("startup: state restored from snapshot", "path", stateFile, "stores", len(env.Stores))
					}
				}
				// Re-seed the GCS generation counter from the restored store so
				// generations stay monotonic across restart (memory mode restores
				// after the provider is constructed).
				storageP.SeedGeneration(ctx)

				var localBlobs *blobfs.LocalFSBlobStore
				if lb, ok := stores.blobs.(*blobfs.LocalFSBlobStore); ok {
					localBlobs = lb
				}
				loopCfg := snapshot.SnapshotLoopConfig{
					Barrier:     barrier,
					Stores:      loopStores,
					BlobStore:   localBlobs,
					DataDir:     dataDir,
					Interval:    cfg.SnapshotInterval,
					Clock:       cfg.Clock,
					SaveTimeout: 10 * time.Second,
				}
				loop := snapshot.NewSnapshotLoop(loopCfg)
				loopCtx, loopCancel := context.WithCancel(ctx)
				loop.Start(loopCtx)
				prevCleanup := cleanup
				cleanup = func() { loopCancel(); loop.Stop(); prevCleanup() }
			}
			defer cleanup()

			srv := gateway.NewServer(cfg, adminHandler, reg, cloudAdapter, certs, gatewayOpts...)

			// Serve gRPC on its own listener (plaintext h2c) alongside the HTTP
			// gateway. Emulator-mode SDKs point FIRESTORE_EMULATOR_HOST here.
			go func() {
				slog.Info("grpc server starting", "grpc_port", grpcPort)
				if err := gserv.Serve(); err != nil {
					slog.Error("grpc server error", "err", err)
				}
			}()
			defer gserv.Stop()

			return srv.ListenAndServe()
		},
	}
	cmd.Flags().Int("port", 8080, "Listen port")
	cmd.Flags().Bool("ephemeral", false, "Run with purely in-memory state (no persistence)")
	cmd.Flags().String("dsn", "", "PostgreSQL connection string (postgres://...)")
	cmd.Flags().String("data-dir", "", "Root data directory for blobs and snapshots")
	cmd.Flags().Bool("fresh-start", false, "Wipe existing state on startup before initializing stores")
	cmd.Flags().String("log-level", "info", "Log level: debug/info/warn/error")
	cmd.Flags().Bool("metrics", false, "Expose Prometheus metrics at /metrics")
	cmd.Flags().Bool("deterministic", false, "Enable deterministic mode")
	cmd.Flags().Int64("seed", 0, "Random seed (requires --deterministic)")
	cmd.Flags().String("time", "", "Base time RFC3339 (requires --deterministic)")
	cmd.Flags().String("time-mode", "offset", "Time mode: frozen or offset")
	cmd.Flags().String("blob-dir", "", "Directory for GCS blob bytes (persistent mode only)")
	cmd.Flags().Bool("gcp-metadata", false, "Enable the GCP metadata-server emulator")
	cmd.Flags().String("kms-master-key", "", "32-byte hex KEK for KMS envelope encryption")
	cmd.Flags().Int("grpc-port", 8081, "gRPC (h2c) listen port")
	return cmd
}

func bindFlags(cmd *cobra.Command) {
	viper.BindPFlag("port", cmd.Flags().Lookup("port"))
	viper.BindPFlag("ephemeral", cmd.Flags().Lookup("ephemeral"))
	viper.BindPFlag("dsn", cmd.Flags().Lookup("dsn"))
	viper.BindPFlag("data_dir", cmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("fresh_start", cmd.Flags().Lookup("fresh-start"))
	viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
	viper.BindPFlag("metrics", cmd.Flags().Lookup("metrics"))
	viper.BindPFlag("deterministic", cmd.Flags().Lookup("deterministic"))
	viper.BindPFlag("seed", cmd.Flags().Lookup("seed"))
	viper.BindPFlag("time", cmd.Flags().Lookup("time"))
	viper.BindPFlag("time_mode", cmd.Flags().Lookup("time-mode"))
	viper.BindPFlag("blob_dir", cmd.Flags().Lookup("blob-dir"))
	viper.BindPFlag("gcp_metadata_enabled", cmd.Flags().Lookup("gcp-metadata"))
	viper.BindPFlag("kms_master_key", cmd.Flags().Lookup("kms-master-key"))
}

// stores bundles the per-mode store backends constructed by initStores.
type stores struct {
	objects   gcs.ObjectStore
	messages  pubsubstore.Messages
	secrets   secretmanagerstore.Store
	keys      kmsstore.Store
	documents firestorestore.FirestoreStore
	resources store.ResourceStore
	blobs     blobfs.BlobStore
	close     func()
}

func initStores(ctx context.Context, cfg *config.Config, instanceID string) (*stores, error) {
	if cfg.DSN != "" {
		pg, err := store.NewPostgresResourceStore(ctx, cfg.DSN, string(cfg.Cloud))
		if err != nil {
			return nil, fmt.Errorf("postgres: %w", err)
		}
		if err := store.RunMigrations(ctx, pg.Pool(), string(cfg.Cloud), gcpstore.MigrationFS, "gcp"); err != nil {
			pg.Close()
			return nil, fmt.Errorf("gcp migrations: %w", err)
		}
		blobs, err := blobfs.NewLocalFSBlobStore(cfg.BlobDir)
		if err != nil {
			pg.Close()
			return nil, fmt.Errorf("blobfs: %w", err)
		}
		return &stores{
			objects:   gcs.NewPostgresObjectStore(pg.Pool()),
			messages:  pubsubstore.NewPostgresMessages(pg.Pool()),
			secrets:   secretmanagerstore.NewPostgresStore(pg.Pool()),
			keys:      kmsstore.NewPostgresStore(pg.Pool()),
			documents: firestorestore.NewPostgresStore(pg.Pool()),
			resources: pg,
			blobs:     blobs,
			close:     func() { pg.Close() },
		}, nil
	}
	if cfg.Ephemeral {
		return &stores{
			objects:   gcs.NewMemoryObjectStore(),
			messages:  pubsubstore.NewMemoryMessages(),
			secrets:   secretmanagerstore.NewMemoryStore(),
			keys:      kmsstore.NewMemoryStore(),
			documents: firestorestore.NewMemoryStore(),
			resources: store.NewMemoryResourceStore(),
			blobs:     blobfs.NewMemoryBlobStore(),
			close:     func() {},
		}, nil
	}
	blobs, err := blobfs.NewSessionBlobStore(instanceID)
	if err != nil {
		return nil, fmt.Errorf("blobfs: %w", err)
	}
	return &stores{
		objects:   gcs.NewMemoryObjectStore(),
		messages:  pubsubstore.NewMemoryMessages(),
		secrets:   secretmanagerstore.NewMemoryStore(),
		keys:      kmsstore.NewMemoryStore(),
		documents: firestorestore.NewMemoryStore(),
		resources: store.NewMemoryResourceStore(),
		blobs:     blobs,
		close:     func() {},
	}, nil
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud-gcp %s\n", version)
		},
	}
}

func envCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "env",
		Short: "Print effective configuration as environment variables",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(model.CloudGCP)
			if err != nil {
				return err
			}
			fmt.Printf("JAISCLOUD_PORT=%d\n", cfg.Port)
			if cfg.Ephemeral {
				fmt.Printf("JAISCLOUD_EPHEMERAL=true\n")
			}
			if cfg.DSN != "" {
				fmt.Printf("JAISCLOUD_DSN=%s\n", cfg.DSN)
			}
			fmt.Printf("JAISCLOUD_CLOUD=gcp\n")
			fmt.Printf("JAISCLOUD_PROJECT_ID=%s\n", cfg.ProjectID)
			fmt.Printf("JAISCLOUD_LOG_LEVEL=%s\n", cfg.LogLevel)
			return nil
		},
	}
}

func doctorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check that the emulator is reachable",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Get(host + "/_jaiscloud/health")
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: cannot reach %s: %v\n", host, err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Fprintf(os.Stderr, "ERROR: health check returned HTTP %d\n", resp.StatusCode)
				os.Exit(1)
			}
			fmt.Printf("OK: jaiscloud-gcp is running at %s\n", host)
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

func resetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Wipe all emulator state",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Post(host+"/_jaiscloud/reset", "application/json", nil)
			if err != nil {
				return fmt.Errorf("reset: %w", err)
			}
			defer resp.Body.Close()
			fmt.Println("State reset.")
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

func exportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export emulator state to a snapshot tarball (or stdout)",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			output, _ := cmd.Flags().GetString("output")

			resp, err := http.Get(host + "/_jaiscloud/export")
			if err != nil {
				return fmt.Errorf("export: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("export failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("read export: %w", err)
			}

			if output == "" || output == "-" {
				_, err = os.Stdout.Write(data)
				return err
			}
			if err := os.WriteFile(output, data, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", output, err)
			}
			sizeMB := float64(len(data)) / (1024 * 1024)
			fmt.Fprintf(os.Stderr, "Export complete -> %s (%.2f MB)\n", output, sizeMB)
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().StringP("output", "o", "-", "Output file (default: stdout)")
	return cmd
}

func importCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import emulator state from a snapshot tarball (or stdin)",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			input, _ := cmd.Flags().GetString("input")
			newInstance, _ := cmd.Flags().GetBool("new-instance")
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			var data []byte
			var err error
			if input == "" || input == "-" {
				data, err = io.ReadAll(os.Stdin)
			} else {
				data, err = os.ReadFile(input)
			}
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}

			importURL := host + "/_jaiscloud/import"
			sep := "?"
			if newInstance {
				importURL += sep + "new_instance=true"
				sep = "&"
			}
			if dryRun {
				importURL += sep + "dry_run=true"
			}

			contentType := "application/json"
			if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
				contentType = "application/x-tar"
			}

			resp, err := http.Post(importURL, contentType, bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("import: %w", err)
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("import failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
			}

			var result map[string]any
			if json.Unmarshal(respBody, &result) == nil {
				if dryRun {
					if valid, _ := result["dry_run"].(bool); valid {
						storeCount, _ := result["stores_parseable"].(float64)
						fmt.Fprintf(os.Stderr, "Dry-run complete: %d stores parseable, no state modified.\n", int(storeCount))
						return nil
					}
				}
				storeCount, _ := result["stores_restored"].(float64)
				fmt.Fprintf(os.Stderr, "Import complete (%d stores restored).\n", int(storeCount))
			} else {
				fmt.Fprintln(os.Stderr, "Import complete.")
			}
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().StringP("input", "i", "-", "Input file (default: stdin)")
	cmd.Flags().Bool("new-instance", false, "Assign a fresh instance ID on import")
	cmd.Flags().Bool("dry-run", false, "Validate the snapshot without modifying state")
	return cmd
}

func snapshotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Manage named snapshots",
	}
	cmd.AddCommand(snapshotCreateCmd())
	cmd.AddCommand(snapshotListCmd())
	cmd.AddCommand(snapshotRevertCmd())
	cmd.AddCommand(snapshotDeleteCmd())
	cmd.AddCommand(snapshotInspectCmd())
	return cmd
}

func snapshotCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a named snapshot of current state",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			name, _ := cmd.Flags().GetString("name")
			desc, _ := cmd.Flags().GetString("description")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			body, _ := json.Marshal(map[string]string{"name": name, "description": desc})
			resp, err := http.Post(host+"/_jaiscloud/snapshot", "application/json", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("snapshot create: %w", err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusCreated {
				return fmt.Errorf("snapshot create failed (HTTP %d): %s", resp.StatusCode, data)
			}
			fmt.Fprintf(os.Stderr, "Snapshot %q created.\n", name)
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().String("name", "", "Snapshot name (required)")
	cmd.Flags().String("description", "", "Optional description")
	return cmd
}

func snapshotListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List named snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Get(host + "/_jaiscloud/snapshots")
			if err != nil {
				return fmt.Errorf("snapshot list: %w", err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("snapshot list failed (HTTP %d): %s", resp.StatusCode, data)
			}
			var metas []map[string]any
			if err := json.Unmarshal(data, &metas); err != nil {
				fmt.Println(string(data))
				return nil
			}
			if len(metas) == 0 {
				fmt.Println("No snapshots found.")
				return nil
			}
			fmt.Printf("%-30s %-25s %s\n", "NAME", "CREATED", "DESCRIPTION")
			for _, m := range metas {
				fmt.Printf("%-30s %-25s %s\n",
					m["name"], m["created_at"], m["description"])
			}
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

func snapshotRevertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revert <name>",
		Short: "Revert to a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resetFirst, _ := cmd.Flags().GetBool("reset-first")
			name := args[0]
			url := host + "/_jaiscloud/snapshot/" + name + "/revert"
			if resetFirst {
				url += "?reset_first=true"
			}
			resp, err := http.Post(url, "application/json", nil)
			if err != nil {
				return fmt.Errorf("snapshot revert: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("snapshot revert failed (HTTP %d): %s", resp.StatusCode, body)
			}
			fmt.Fprintf(os.Stderr, "Reverted to snapshot %q.\n", name)
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().Bool("reset-first", false, "Reset state before reverting")
	return cmd
}

func snapshotDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			yes, _ := cmd.Flags().GetBool("yes")
			name := args[0]
			if !yes {
				return fmt.Errorf("pass --yes to confirm deletion of snapshot %q", name)
			}
			req, _ := http.NewRequest(http.MethodDelete,
				host+"/_jaiscloud/snapshot/"+name+"?yes=true", nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("snapshot delete: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("snapshot delete failed (HTTP %d): %s", resp.StatusCode, body)
			}
			fmt.Fprintf(os.Stderr, "Snapshot %q deleted.\n", name)
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().Bool("yes", false, "Confirm deletion")
	return cmd
}

func snapshotInspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect <name>",
		Short: "Inspect a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			name := args[0]
			resp, err := http.Get(host + "/_jaiscloud/snapshot/" + name)
			if err != nil {
				return fmt.Errorf("snapshot inspect: %w", err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("snapshot inspect failed (HTTP %d): %s", resp.StatusCode, data)
			}
			var pretty json.RawMessage
			if json.Unmarshal(data, &pretty) == nil {
				if indented, err := json.MarshalIndent(pretty, "", "  "); err == nil {
					fmt.Println(string(indented))
					return nil
				}
			}
			fmt.Println(string(data))
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

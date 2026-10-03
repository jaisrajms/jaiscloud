package main

import (
	"fmt"
	"net/http"
	"os"

	"jaiscloud/internal/admin"
	azureadapter "jaiscloud/internal/azure/adapter"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gateway"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const version = "0.2.0"

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud-azure",
		Short: "JaisCloud Azure - local Azure emulator (stub)",
	}
	root.AddCommand(startCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(doctorCmd())
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
			viper.BindPFlag("port", cmd.Flags().Lookup("port"))
			viper.BindPFlag("mode", cmd.Flags().Lookup("mode"))
			viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))

			cfg, err := config.Load(model.CloudAzure)
			if err != nil {
				return err
			}

			stateDir, _ := config.ResolveStateDir(os.Getenv("JAISCLOUD_STATE_DIR"))
			var certs certstore.CertStore
			if fsCS, err := certstore.NewFilesystemCertStore(stateDir); err == nil {
				certs = fsCS
			} else {
				certs = certstore.NewMemoryCertStore()
			}

			adminHandler := admin.NewHandler()
			adminHandler.SetMeta(admin.HandlerMeta{
				Cloud:     "azure",
				Region:    cfg.Region,
				AccountID: cfg.AccountID,
				StateDir:  stateDir,
			})

			reg := provider.NewRegistry()
			cloudAdapter := azureadapter.New()
			srv := gateway.NewServer(cfg, adminHandler, reg, cloudAdapter, certs)
			return srv.ListenAndServe()
		},
	}
	cmd.Flags().Int("port", 4566, "Listen port")
	cmd.Flags().String("mode", "lite", "Mode: lite or full")
	cmd.Flags().String("log-level", "info", "Log level: debug/info/warn/error")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud-azure %s\n", version)
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
			fmt.Printf("OK: jaiscloud-azure is running at %s\n", host)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

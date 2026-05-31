package ui

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/admin"
	adminpanelui "jaiscloud/internal/aws/ui/adminpanel"
	dynamodbui "jaiscloud/internal/aws/ui/dynamodb"
	iamui "jaiscloud/internal/aws/ui/iam"
	kmsui "jaiscloud/internal/aws/ui/kms"
	lambdaui "jaiscloud/internal/aws/ui/lambda"
	logsui "jaiscloud/internal/aws/ui/logs"
	s3ui "jaiscloud/internal/aws/ui/s3"
	secretsmanagerui "jaiscloud/internal/aws/ui/secretsmanager"
	snsui "jaiscloud/internal/aws/ui/sns"
	sqsui "jaiscloud/internal/aws/ui/sqs"
	ssmui "jaiscloud/internal/aws/ui/ssm"
	"jaiscloud/internal/aws/ui/middleware"
	"jaiscloud/internal/aws/ui/sse"
	"jaiscloud/internal/config"
)

// MetaResponse is the payload for GET /api/ui/v1/meta.
type MetaResponse struct {
	Cloud      string `json:"cloud"`
	Region     string `json:"region"`
	AccountId  string `json:"accountId"`
	Mode       string `json:"mode"`       // "memory" | "postgres" | "ephemeral"
	Version    string `json:"version"`
	UIVersion  string `json:"uiVersion"`
	InstanceId string `json:"instanceId"`
}

// ServicesResponse is the payload for GET /api/ui/v1/services.
type ServicesResponse struct {
	Services []string `json:"services"`
}

// BuildRouter builds the UI chi router.
// version is the binary version string (injected from main.go via -ldflags or "dev").
func BuildRouter(
	providers *AWSProviders,
	adminHandler *admin.Handler,
	broker *sse.Broker,
	cfg *config.Config,
	token string,
	version string,
) chi.Router {
	r := chi.NewRouter()

	r.Use(middleware.InjectConfig(cfg.Region, cfg.AccountID))
	r.Use(middleware.CORS(cfg))

	assets, _ := StaticFS()
	if assets != nil {
		r.Get("/ui/*", spaHandler(assets, token))
		r.Get("/ui", http.RedirectHandler("/ui/", http.StatusMovedPermanently).ServeHTTP)
	}

	// Not auth-protected — SPA probes these before the session cookie is set.
	r.Get("/api/ui/v1/meta", buildMetaHandler(adminHandler, cfg, version))
	r.Get("/api/ui/v1/meta/accounts", buildAccountsHandler(cfg))
	r.Get("/api/ui/v1/services", buildServicesHandler(providers))

	r.Group(func(r chi.Router) {
		r.Use(middleware.Auth(token))

		r.Get("/api/ui/v1/events/stream", broker.ServeHTTP)

		// Phase 0 service routes
		if providers.Queue != nil {
			r.Mount("/api/ui/v1/sqs", sqsui.BuildRouter(providers.Queue, cfg))
		}
		if providers.Function != nil {
			r.Mount("/api/ui/v1/lambda", lambdaui.BuildRouter(providers.Function, cfg))
		}
		if providers.Logs != nil {
			r.Mount("/api/ui/v1/logs", logsui.BuildRouter(providers.Logs, cfg))
		}

		// Phase 1a service routes
		if providers.Object != nil {
			r.Mount("/api/ui/v1/s3", s3ui.BuildRouter(providers.Object, cfg))
		}
		if providers.Table != nil {
			r.Mount("/api/ui/v1/dynamodb", dynamodbui.BuildRouter(providers.Table, cfg))
		}
		if providers.Notif != nil {
			r.Mount("/api/ui/v1/sns", snsui.BuildRouter(providers.Notif, cfg))
		}
		r.Mount("/api/ui/v1/admin", adminpanelui.BuildRouter(adminHandler, cfg))

		// Phase 1b service routes
		if providers.IAM != nil {
			r.Mount("/api/ui/v1/iam", iamui.BuildRouter(providers.IAM, cfg))
		}
		if providers.Key != nil {
			r.Mount("/api/ui/v1/kms", kmsui.BuildRouter(providers.Key, cfg))
		}
		if providers.Secret != nil {
			r.Mount("/api/ui/v1/secretsmanager", secretsmanagerui.BuildRouter(providers.Secret, cfg))
		}
		if providers.Param != nil {
			r.Mount("/api/ui/v1/ssm", ssmui.BuildRouter(providers.Param, cfg))
		}
	})

	return r
}

// spaHandler serves all /ui/* paths from the embedded dist filesystem.
// It sets the session cookie on every response so EventSource can authenticate.
// Must NOT be implemented as plain http.FileServer — it would not set the cookie.
func spaHandler(assets fs.FS, token string) http.HandlerFunc {
	fileServer := http.FileServer(http.FS(assets))
	return func(w http.ResponseWriter, r *http.Request) {
		// Set session cookie so EventSource (which cannot set custom headers) can auth.
		http.SetCookie(w, &http.Cookie{
			Name:     "session",
			Value:    token,
			Path:     "/",
			MaxAge:   86400,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})

		// Strip the /ui prefix so the file server can find assets at their actual paths.
		r2 := r.Clone(r.Context())
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/ui")
		if r2.URL.Path == "" {
			r2.URL.Path = "/"
		}

		// For SPA routing: serve index.html for any path that doesn't match a file.
		if _, err := fs.Stat(assets, strings.TrimPrefix(r2.URL.Path, "/")); err != nil {
			r2.URL.Path = "/"
		}

		fileServer.ServeHTTP(w, r2)
	}
}

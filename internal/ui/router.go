package ui

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
	"jaiscloud/internal/ui/adminpanel"
	"jaiscloud/internal/ui/middleware"
	"jaiscloud/internal/ui/sse"
)

// BuildRouter builds the cloud-neutral UI chi router. The registrar supplies
// the cloud identity, the service catalog and the service API routes.
// version is the binary version string (injected from main.go via -ldflags or
// "dev") and bootID is the per-process restart marker surfaced via /meta.
func BuildRouter(
	reg Registrar,
	adminHandler *admin.Handler,
	broker *sse.Broker,
	cfg *config.Config,
	token string,
	version string,
	bootID string,
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
	r.Get("/api/ui/v1/meta", buildMetaHandler(adminHandler, cfg, version, string(reg.Cloud()), bootID))
	r.Get("/api/ui/v1/meta/accounts", buildAccountsHandler(cfg, reg))
	r.Get("/api/ui/v1/services", buildServicesHandler(reg))

	r.Group(func(r chi.Router) {
		r.Use(middleware.Auth(token))

		r.Get("/api/ui/v1/events/stream", broker.ServeHTTP)

		// Cloud-neutral admin plane: the shared admin.Handler is the same for
		// every cloud, so the panel is mounted once in the core rather than by
		// each Registrar. Handlers assume a valid session token.
		r.Mount("/api/ui/v1/admin", adminpanel.BuildRouter(adminHandler))

		// Cloud-specific service routes.
		reg.MountRoutes(r)
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

		// Hash-named build assets are immutable; HTML and SPA fallbacks must be
		// revalidated so a new build is never masked by the browser cache.
		if strings.HasPrefix(r2.URL.Path, "/assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}

		fileServer.ServeHTTP(w, r2)
	}
}

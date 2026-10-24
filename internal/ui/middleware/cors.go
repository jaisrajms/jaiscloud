package middleware

import (
	"fmt"
	"net/http"

	"jaiscloud/internal/config"
)

// CORS restricts cross-origin requests to localhost:<uiPort>.
// In dev mode (cfg.UIDevMode), also allows cfg.DevUIOrigin (default http://localhost:5173).
// Never allows "*".
func CORS(cfg *config.Config) func(http.Handler) http.Handler {
	localOrigin := fmt.Sprintf("http://localhost:%d", cfg.UIPort)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			allowed := false
			if origin == localOrigin {
				allowed = true
			} else if cfg.UIDevMode && cfg.DevUIOrigin != "" && origin == cfg.DevUIOrigin {
				allowed = true
			}
			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

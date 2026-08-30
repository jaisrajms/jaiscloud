package middleware

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Auth verifies every /api/ui/v1/* request carries a valid session token.
// Accepts either:
//   - Authorization: Bearer <token>  (XHR, fetch)
//   - Cookie: session=<token>        (EventSource — browser sends automatically)
//
// Responds 401 { "code":"Unauthorized","message":"..." } on failure.
func Auth(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !checkToken(r, token) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
					"code":    "Unauthorized",
					"message": "missing or invalid session token",
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func checkToken(r *http.Request, expected string) bool {
	if expected == "" {
		return false
	}
	// Bearer header
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ") == expected
	}
	// Session cookie
	if c, err := r.Cookie("session"); err == nil {
		return c.Value == expected
	}
	// Dev mode: ?token= query param (for EventSource which cannot set headers)
	if t := r.URL.Query().Get("token"); t != "" {
		return t == expected
	}
	return false
}

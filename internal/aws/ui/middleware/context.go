package middleware

import (
	"context"
	"net/http"
)

// CtxKey is the context key type for values injected by this middleware.
type CtxKey string

const (
	// CtxKeyRegion is the context key for the injected AWS region.
	CtxKeyRegion CtxKey = "region"
	// CtxKeyAccount is the context key for the injected AWS account ID.
	CtxKeyAccount CtxKey = "account"
)

// InjectConfig injects region and accountID into every request context so
// regionFrom(r) and accountFrom(r) in the ui package work correctly.
func InjectConfig(region, accountID string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), CtxKeyRegion, region)
			ctx = context.WithValue(ctx, CtxKeyAccount, accountID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

package middleware

import (
	"context"
	"net/http"
)

type contextKey string

const userIDKey contextKey = "user_id"

// WithUserID reads the X-User-Id header the value into the request context.
func WithUserID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("X-User-Id")
		ctx := context.WithValue(r.Context(), userIDKey, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetUserID retrieves the authenticated user ID from the context.
// Returns an empty string if not set.
func GetUserID(ctx context.Context) string {
	if id, ok := ctx.Value(userIDKey).(string); ok {
		return id
	}
	return ""
}

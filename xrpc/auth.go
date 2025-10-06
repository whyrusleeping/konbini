package xrpc

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// requireAuth is middleware that requires authentication
func (s *Server) requireAuth(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		viewer, err := s.authenticate(c)
		if err != nil {
			return XRPCError(c, http.StatusUnauthorized, "AuthenticationRequired", err.Error())
		}
		c.Set("viewer", viewer)
		return next(c)
	}
}

// optionalAuth is middleware that optionally authenticates
func (s *Server) optionalAuth(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		viewer, _ := s.authenticate(c)
		if viewer != "" {
			c.Set("viewer", viewer)
		}
		return next(c)
	}
}

// authenticate extracts and validates the JWT from the Authorization header
// Returns the viewer DID if valid, empty string otherwise
func (s *Server) authenticate(c echo.Context) (string, error) {
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing authorization header")
	}

	// Extract Bearer token
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return "", fmt.Errorf("invalid authorization header format")
	}

	tokenString := parts[1]

	// Parse JWT without signature validation (for development)
	// In production, you'd want to validate the signature using the issuer's public key
	token, err := jwt.Parse([]byte(tokenString), jwt.WithVerify(false), jwt.WithValidate(false))
	if err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	// Extract the user's DID - try both "sub" (PDS tokens) and "iss" (service tokens)
	var userDID string

	// First try "sub" claim (used by PDS tokens and entryway tokens)
	sub := token.Subject()
	if sub != "" && strings.HasPrefix(sub, "did:") {
		userDID = sub
	} else {
		// Fall back to "iss" claim (used by some service tokens)
		iss := token.Issuer()
		if iss != "" && strings.HasPrefix(iss, "did:") {
			userDID = iss
		}
	}

	if userDID == "" {
		return "", fmt.Errorf("missing 'sub' or 'iss' claim with DID in token")
	}

	// Optional: check scope if present
	scope, ok := token.Get("scope")
	if ok {
		scopeStr, _ := scope.(string)
		// Valid scopes are: com.atproto.access, com.atproto.appPass, com.atproto.appPassPrivileged
		if scopeStr != "com.atproto.access" && scopeStr != "com.atproto.appPass" && scopeStr != "com.atproto.appPassPrivileged" {
			return "", fmt.Errorf("invalid token scope: %s", scopeStr)
		}
	}

	return userDID, nil
}

// resolveActor resolves an actor identifier (handle or DID) to a DID
func (s *Server) resolveActor(ctx context.Context, actor string) (string, error) {
	// If it's already a DID, return it
	if strings.HasPrefix(actor, "did:") {
		return actor, nil
	}

	// Otherwise, resolve the handle
	resp, err := s.dir.LookupHandle(ctx, syntax.Handle(actor))
	if err != nil {
		return "", fmt.Errorf("failed to resolve handle: %w", err)
	}

	return resp.DID.String(), nil
}

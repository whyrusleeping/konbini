package xrpc

import (
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
)

// handleResolveHandle implements com.atproto.identity.resolveHandle
func (s *Server) handleResolveHandle(c echo.Context) error {
	handle := c.QueryParam("handle")
	if handle == "" {
		return XRPCError(c, http.StatusBadRequest, "InvalidRequest", "handle parameter is required")
	}

	// Clean up handle (remove @ prefix if present)
	handle = strings.TrimPrefix(handle, "@")

	// Resolve handle to DID
	resp, err := s.dir.LookupHandle(c.Request().Context(), syntax.Handle(handle))
	if err != nil {
		return XRPCError(c, http.StatusBadRequest, "HandleNotFound", "handle not found")
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"did": resp.DID.String(),
	})
}

package graph

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"gorm.io/gorm"
)

// HandleGetMutes implements app.bsky.graph.getMutes
// NOTE: Mutes are typically stored as user preferences/settings, not as repo records.
// This implementation returns an empty list as mute tracking is not yet implemented
// in the database schema.
func HandleGetMutes(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	// Get viewer from authentication
	viewer := c.Get("viewer")
	if viewer == nil {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	// TODO: Implement mute tracking in the database
	// Mutes are different from blocks - they're typically stored as preferences
	// rather than as repo records. Would need a new table like:
	// CREATE TABLE user_mutes (
	//   id SERIAL PRIMARY KEY,
	//   actor_did TEXT NOT NULL,
	//   muted_did TEXT NOT NULL,
	//   created_at TIMESTAMP NOT NULL,
	//   UNIQUE(actor_did, muted_did)
	// );

	// For now, return empty list
	return c.JSON(http.StatusOK, map[string]interface{}{
		"mutes":  []interface{}{},
		"cursor": "",
	})
}

package actor

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"gorm.io/gorm"
)

// HandlePutPreferences implements app.bsky.actor.putPreferences
// Stubbed out for now - just returns success without doing anything
func HandlePutPreferences(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	// Get viewer from authentication
	viewer := c.Get("viewer")
	if viewer == nil {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	// For now, just return success without storing anything
	return c.JSON(http.StatusOK, map[string]interface{}{})
}

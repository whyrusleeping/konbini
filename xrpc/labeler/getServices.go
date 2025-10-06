package labeler

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// HandleGetServices implements app.bsky.labeler.getServices
// Returns information about labeler services
func HandleGetServices(c echo.Context) error {
	// For now, return empty views since we don't have labeler support
	// A full implementation would parse the "dids" query parameter
	return c.JSON(http.StatusOK, map[string]interface{}{
		"views": []interface{}{},
	})
}

package unspecced

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// HandleGetConfig implements app.bsky.unspecced.getConfig
// Returns basic configuration for the app
func HandleGetConfig(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"checkEmailConfirmed": false,
		"liveNow":             []any{},
	})
}

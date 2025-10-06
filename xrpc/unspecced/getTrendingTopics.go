package unspecced

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// HandleGetTrendingTopics implements app.bsky.unspecced.getTrendingTopics
// Returns trending topics (empty for now)
func HandleGetTrendingTopics(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"topics":    []interface{}{},
		"suggested": []interface{}{},
	})
}

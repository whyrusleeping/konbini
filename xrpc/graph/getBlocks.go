package graph

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetBlocks implements app.bsky.graph.getBlocks
func HandleGetBlocks(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	// Get viewer from authentication
	viewer := c.Get("viewer")
	if viewer == nil {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}
	viewerDID := viewer.(string)

	// Parse limit
	limit := 50
	if limitParam := c.QueryParam("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	// Parse cursor (block ID)
	var cursor uint
	if cursorParam := c.QueryParam("cursor"); cursorParam != "" {
		if c, err := strconv.ParseUint(cursorParam, 10, 64); err == nil {
			cursor = uint(c)
		}
	}

	ctx := c.Request().Context()

	// Query blocks
	type blockRow struct {
		ID         uint
		SubjectDid string
	}
	var rows []blockRow

	query := `
		SELECT b.id, r.did as subject_did
		FROM blocks b
		LEFT JOIN repos r ON r.id = b.subject
		WHERE b.author = (SELECT id FROM repos WHERE did = ?)
	`
	if cursor > 0 {
		query += ` AND b.id < ?`
	}
	query += ` ORDER BY b.id DESC LIMIT ?`

	var queryArgs []interface{}
	queryArgs = append(queryArgs, viewerDID)
	if cursor > 0 {
		queryArgs = append(queryArgs, cursor)
	}
	queryArgs = append(queryArgs, limit)

	if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to query blocks",
		})
	}

	// Hydrate blocked actors
	blocks := make([]interface{}, 0)
	for _, row := range rows {
		actorInfo, err := hydrator.HydrateActor(ctx, row.SubjectDid)
		if err != nil {
			fmt.Println("Hydrating actor failed: ", err)
			continue
		}
		blocks = append(blocks, views.ProfileView(actorInfo))
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		nextCursor = strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"blocks": blocks,
		"cursor": nextCursor,
	})
}

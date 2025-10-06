package feed

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetRepostedBy implements app.bsky.feed.getRepostedBy
func HandleGetRepostedBy(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	uriParam := c.QueryParam("uri")
	if uriParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "uri parameter is required",
		})
	}

	// Parse limit
	limit := 50
	if limitParam := c.QueryParam("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	// Parse cursor (repost ID)
	var cursor uint
	if cursorParam := c.QueryParam("cursor"); cursorParam != "" {
		if c, err := strconv.ParseUint(cursorParam, 10, 64); err == nil {
			cursor = uint(c)
		}
	}

	ctx := c.Request().Context()

	// Get post ID from URI
	var postID uint
	db.Raw(`
		SELECT id FROM posts
		WHERE author = (SELECT id FROM repos WHERE did = ?)
		AND rkey = ?
	`, extractDIDFromURI(uriParam), extractRkeyFromURI(uriParam)).Scan(&postID)

	if postID == 0 {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "NotFound",
			"message": "post not found",
		})
	}

	// Query reposts
	type repostRow struct {
		ID        uint
		AuthorDid string
		Rkey      string
		Created   string
	}
	var rows []repostRow

	query := `
		SELECT rp.id, r.did as author_did, rp.rkey, rp.created
		FROM reposts rp
		JOIN repos r ON r.id = rp.author
		WHERE rp.subject = ?
	`
	if cursor > 0 {
		query += ` AND rp.id < ?`
	}
	query += ` ORDER BY rp.id DESC LIMIT ?`

	var queryArgs []interface{}
	queryArgs = append(queryArgs, postID)
	if cursor > 0 {
		queryArgs = append(queryArgs, cursor)
	}
	queryArgs = append(queryArgs, limit)

	if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to query reposts",
		})
	}

	// Hydrate actors
	repostedBy := make([]interface{}, 0)
	for _, row := range rows {
		actorInfo, err := hydrator.HydrateActor(ctx, row.AuthorDid)
		if err != nil {
			continue
		}
		repostedBy = append(repostedBy, views.ProfileView(actorInfo))
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		nextCursor = strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"uri":        uriParam,
		"repostedBy": repostedBy,
		"cursor":     nextCursor,
	})
}

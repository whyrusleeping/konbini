package feed

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetLikes implements app.bsky.feed.getLikes
func HandleGetLikes(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
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

	// Parse cursor (like ID)
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

	// Query likes
	type likeRow struct {
		ID        uint
		AuthorDid string
		Rkey      string
		Created   string
	}
	var rows []likeRow

	query := `
		SELECT l.id, r.did as author_did, l.rkey, l.created
		FROM likes l
		JOIN repos r ON r.id = l.author
		WHERE l.subject = ?
	`
	if cursor > 0 {
		query += ` AND l.id < ?`
	}
	query += ` ORDER BY l.id DESC LIMIT ?`

	var queryArgs []interface{}
	queryArgs = append(queryArgs, postID)
	if cursor > 0 {
		queryArgs = append(queryArgs, cursor)
	}
	queryArgs = append(queryArgs, limit)

	if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to query likes",
		})
	}

	// Hydrate actors
	likes := make([]interface{}, 0)
	for _, row := range rows {
		actorInfo, err := hydrator.HydrateActor(ctx, row.AuthorDid)
		if err != nil {
			continue
		}

		like := map[string]interface{}{
			"actor":     views.ProfileView(actorInfo),
			"createdAt": row.Created,
			"indexedAt": row.Created,
		}
		likes = append(likes, like)
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		nextCursor = strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"uri":    uriParam,
		"likes":  likes,
		"cursor": nextCursor,
	})
}

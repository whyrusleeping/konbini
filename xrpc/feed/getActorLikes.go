package feed

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetActorLikes implements app.bsky.feed.getActorLikes
func HandleGetActorLikes(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	actorParam := c.QueryParam("actor")
	if actorParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "actor parameter is required",
		})
	}

	ctx := c.Request().Context()

	// Resolve actor to DID
	actorDID, err := hydrator.ResolveDID(ctx, actorParam)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "ActorNotFound",
			"message": "actor not found",
		})
	}

	// Check authentication - user can only view their own likes
	viewer := c.Get("viewer")
	if viewer == nil || viewer.(string) != actorDID {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "you can only view your own likes",
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

	// Query likes
	type likeRow struct {
		ID      uint
		Subject string // post URI
	}
	var rows []likeRow

	query := `
		SELECT l.id, 'at://' || r.did || '/app.bsky.feed.post/' || p.rkey as subject
		FROM likes l
		JOIN posts p ON p.id = l.subject
		JOIN repos r ON r.id = p.author
		WHERE l.author = (SELECT id FROM repos WHERE did = ?)
	`
	if cursor > 0 {
		query += ` AND l.id < ?`
	}
	query += ` ORDER BY l.id DESC LIMIT ?`

	var queryArgs []interface{}
	queryArgs = append(queryArgs, actorDID)
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

	// Hydrate posts
	feed := make([]interface{}, 0)
	for _, row := range rows {
		postInfo, err := hydrator.HydratePost(ctx, row.Subject, actorDID)
		if err != nil {
			continue
		}

		// Hydrate the post author
		authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
		if err != nil {
			continue
		}

		feed = append(feed, views.FeedViewPost(postInfo, authorInfo))
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		nextCursor = strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"feed":   feed,
		"cursor": nextCursor,
	})
}

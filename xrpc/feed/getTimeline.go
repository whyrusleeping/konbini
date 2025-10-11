package feed

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
)

var tracer = otel.Tracer("xrpc/feed")

// HandleGetTimeline implements app.bsky.feed.getTimeline
func HandleGetTimeline(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	ctx := c.Request().Context()
	ctx, span := tracer.Start(ctx, "getTimeline")
	defer span.End()

	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]any{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	// Parse limit
	limit := 50
	if limitParam := c.QueryParam("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	// Parse cursor (timestamp)
	cursor := time.Now()
	if cursorParam := c.QueryParam("cursor"); cursorParam != "" {
		if t, err := time.Parse(time.RFC3339, cursorParam); err == nil {
			cursor = t
		}
	}

	// Get viewer's repo ID
	var viewerRepoID uint
	if err := db.Raw("SELECT id FROM repos WHERE did = ?", viewer).Scan(&viewerRepoID).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to load viewer",
		})
	}

	// Query posts from followed users

	rows, err := getTimelinePosts(ctx, db, viewerRepoID, cursor, limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to query timeline",
		})
	}

	// Hydrate posts
	feed := hydratePostRows(ctx, hydrator, viewer, rows)

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		// Get the created time of the last post
		var lastCreated time.Time
		lastURI := rows[len(rows)-1].URI
		postInfo, err := hydrator.HydratePost(ctx, lastURI, viewer)
		if err == nil && postInfo.Post != nil {
			t, err := time.Parse(time.RFC3339, postInfo.Post.CreatedAt)
			if err == nil {
				lastCreated = t
				nextCursor = lastCreated.Format(time.RFC3339)
			}
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"feed":   feed,
		"cursor": nextCursor,
	})
}

func getTimelinePosts(ctx context.Context, db *gorm.DB, uid uint, cursor time.Time, limit int) ([]postRow, error) {
	ctx, span := tracer.Start(ctx, "getTimelineQuery")
	defer span.End()

	var rows []postRow
	err := db.Raw(`
		SELECT
			'at://' || r.did || '/app.bsky.feed.post/' || p.rkey as uri,
			p.author as author_id
		FROM posts p
		JOIN repos r ON r.id = p.author
		WHERE p.reply_to = 0
		AND p.author IN (SELECT subject FROM follows WHERE author = ?)
		AND p.created < ?
		AND p.not_found = false
		ORDER BY p.created DESC
		LIMIT ?
	`, uid, cursor, limit).Scan(&rows).Error

	if err != nil {
		return nil, err
	}
	return rows, nil
}

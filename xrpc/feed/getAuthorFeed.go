package feed

import (
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetAuthorFeed implements app.bsky.feed.getAuthorFeed
func HandleGetAuthorFeed(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	actorParam := c.QueryParam("actor")
	if actorParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "actor parameter is required",
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

	// Parse filter (posts_with_replies, posts_no_replies, posts_with_media, etc.)
	filter := c.QueryParam("filter")
	if filter == "" {
		filter = "posts_with_replies" // default
	}

	ctx := c.Request().Context()
	viewer := getUserDID(c)

	// Resolve actor to DID
	did, err := hydrator.ResolveDID(ctx, actorParam)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "ActorNotFound",
			"message": "actor not found",
		})
	}

	// Build query based on filter
	var query string
	switch filter {
	case "posts_no_replies", "posts_and_author_threads":
		query = `
			SELECT
				'at://' || r.did || '/app.bsky.feed.post/' || p.rkey as uri,
				p.author as author_id
			FROM posts p
			JOIN repos r ON r.id = p.author
			WHERE p.author = (SELECT id FROM repos WHERE did = ?)
			AND p.reply_to = 0
			AND p.created < ?
			AND p.not_found = false
			ORDER BY p.created DESC
			LIMIT ?
		`
	default: // posts_with_replies
		query = `
			SELECT
				'at://' || r.did || '/app.bsky.feed.post/' || p.rkey as uri,
				p.author as author_id
			FROM posts p
			JOIN repos r ON r.id = p.author
			WHERE p.author = (SELECT id FROM repos WHERE did = ?)
			AND p.created < ?
			AND p.not_found = false
			ORDER BY p.created DESC
			LIMIT ?
		`
	}

	type postRow struct {
		URI      string
		AuthorID uint
	}
	var rows []postRow
	if err := db.Raw(query, did, cursor, limit).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to query author feed",
		})
	}

	// Hydrate posts
	feed := make([]interface{}, 0)
	for _, row := range rows {
		postInfo, err := hydrator.HydratePost(ctx, row.URI, viewer)
		if err != nil {
			continue
		}

		// Hydrate author
		authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
		if err != nil {
			continue
		}

		feedItem := views.FeedViewPost(postInfo, authorInfo)
		feed = append(feed, feedItem)
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		lastURI := rows[len(rows)-1].URI
		postInfo, err := hydrator.HydratePost(ctx, lastURI, viewer)
		if err == nil && postInfo.Post != nil {
			t, err := time.Parse(time.RFC3339, postInfo.Post.CreatedAt)
			if err == nil {
				nextCursor = t.Format(time.RFC3339)
			}
		}
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"feed":   feed,
		"cursor": nextCursor,
	})
}

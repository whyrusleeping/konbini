package feed

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

type postRow struct {
	URI      string
	AuthorID uint
}

// HandleGetAuthorFeed implements app.bsky.feed.getAuthorFeed
func HandleGetAuthorFeed(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	actorParam := c.QueryParam("actor")
	if actorParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
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
		return c.JSON(http.StatusBadRequest, map[string]any{
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

	var rows []postRow
	if err := db.Raw(query, did, cursor, limit).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to query author feed",
		})
	}

	feed := hydratePostRows(ctx, hydrator, viewer, rows)

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

	return c.JSON(http.StatusOK, map[string]any{
		"feed":   feed,
		"cursor": nextCursor,
	})
}

func hydratePostRows(ctx context.Context, hydrator *hydration.Hydrator, viewer string, rows []postRow) []*bsky.FeedDefs_FeedViewPost {
	ctx, span := tracer.Start(ctx, "hydratePostRows")
	defer span.End()

	// Hydrate posts
	var wg sync.WaitGroup

	var outLk sync.Mutex
	feed := make([]*bsky.FeedDefs_FeedViewPost, len(rows))
	for i, row := range rows {
		wg.Add(1)
		go func(i int, row postRow) {
			defer wg.Done()

			postInfo, err := hydrator.HydratePost(ctx, row.URI, viewer)
			if err != nil {
				if strings.Contains(err.Error(), "post not found") {
					hydrator.AddMissingRecord(row.URI, true)
					postInfo, err = hydrator.HydratePost(ctx, row.URI, viewer)
					if err != nil {
						slog.Error("failed to hydrate post after fetch missing", "uri", row.URI, "error", err)
						return
					}
				} else {
					slog.Warn("failed to hydrate post", "uri", row.URI, "error", err)
					return
				}
			}

			authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
			if err != nil {
				hydrator.AddMissingRecord(postInfo.Author, false)
				slog.Warn("failed to hydrate author", "did", postInfo.Author, "error", err)
				return
			}

			feedItem := views.FeedViewPost(postInfo, authorInfo)
			outLk.Lock()
			feed[i] = feedItem
			outLk.Unlock()
		}(i, row)
	}
	wg.Wait()

	x := 0
	for i := 0; i < len(feed); i++ {
		if feed[i] != nil {
			feed[x] = feed[i]
			x++
			continue
		}
	}
	feed = feed[:x]

	return feed
}

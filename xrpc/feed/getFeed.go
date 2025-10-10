package feed

import (
	"bytes"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"github.com/whyrusleeping/market/models"
	"gorm.io/gorm"
)

// HandleGetFeed implements app.bsky.feed.getFeed
// Gets posts from a custom feed generator
func HandleGetFeed(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator, dir identity.Directory) error {
	// Parse parameters
	feedURI := c.QueryParam("feed")
	if feedURI == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":   "InvalidRequest",
			"message": "feed parameter is required",
		})
	}

	// Parse limit
	limit := int64(50)
	if limitParam := c.QueryParam("limit"); limitParam != "" {
		if l, err := strconv.ParseInt(limitParam, 10, 64); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	// Parse cursor
	cursor := c.QueryParam("cursor")

	ctx := c.Request().Context()
	viewer := getUserDID(c)

	// Extract feed generator DID and rkey from URI
	// URI format: at://did:plc:xxx/app.bsky.feed.generator/rkey
	did := extractDIDFromURI(feedURI)
	rkey := extractRkeyFromURI(feedURI)

	if did == "" || rkey == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":   "InvalidRequest",
			"message": "invalid feed URI format",
		})
	}

	// Check if feed generator exists in database
	var feedGen models.FeedGenerator
	if err := db.Raw(`
		SELECT * FROM feed_generators fg WHERE fg.author = (select id from repos where did = ?) AND fg.rkey = ?
	`, did, rkey).Scan(&feedGen).Error; err != nil {
		return err
	}

	if feedGen.ID == 0 {
		hydrator.AddMissingRecord(feedURI, true)
		return c.JSON(http.StatusNotFound, map[string]any{
			"error":   "NotFound",
			"message": "feed generator not found",
		})
	}

	// Decode the feed generator record to get the service DID
	var feedGenRecord bsky.FeedGenerator
	if err := feedGenRecord.UnmarshalCBOR(bytes.NewReader(feedGen.Raw)); err != nil {
		slog.Error("failed to decode feed generator record", "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to decode feed generator record",
		})
	}

	// Parse the service DID
	serviceDID, err := syntax.ParseDID(feedGenRecord.Did)
	if err != nil {
		slog.Error("invalid service DID in feed generator", "error", err, "did", feedGenRecord.Did)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "invalid service DID",
		})
	}

	// Resolve the service DID to get its endpoint
	serviceIdent, err := dir.LookupDID(ctx, serviceDID)
	if err != nil {
		slog.Error("failed to resolve service DID", "error", err, "did", serviceDID)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to resolve service endpoint",
		})
	}

	serviceEndpoint := serviceIdent.GetServiceEndpoint("bsky_fg")
	if serviceEndpoint == "" {
		slog.Error("service has no bsky_fg endpoint", "did", serviceDID)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "service has no endpoint",
		})
	}

	// Create XRPC client for the feed generator service
	// Pass through headers from the original request so feed generators can
	// customize feeds based on the viewer
	headers := make(map[string]string)

	// Set User-Agent to identify konbini
	headers["User-Agent"] = "konbini/0.0.1"

	// Pass through Authorization header if present (for authenticated feed requests)
	if authHeader := c.Request().Header.Get("Authorization"); authHeader != "" {
		headers["Authorization"] = authHeader
	}

	// Pass through Accept-Language header if present
	if langHeader := c.Request().Header.Get("Accept-Language"); langHeader != "" {
		headers["Accept-Language"] = langHeader
	}

	// Pass through X-Bsky-Topics header if present
	if topicsHeader := c.Request().Header.Get("X-Bsky-Topics"); topicsHeader != "" {
		headers["X-Bsky-Topics"] = topicsHeader
	}

	client := &xrpc.Client{
		Host:    serviceEndpoint,
		Headers: headers,
	}

	// Call getFeedSkeleton on the service
	skeleton, err := bsky.FeedGetFeedSkeleton(ctx, client, cursor, feedURI, limit)
	if err != nil {
		slog.Error("failed to call getFeedSkeleton", "error", err, "service", serviceEndpoint)
		// Return empty feed on error rather than failing completely
		return c.JSON(http.StatusOK, &bsky.FeedGetFeed_Output{
			Feed: make([]*bsky.FeedDefs_FeedViewPost, 0),
		})
	}

	// Hydrate the posts from the skeleton
	posts := make([]*bsky.FeedDefs_FeedViewPost, 0, len(skeleton.Feed))
	var wg sync.WaitGroup
	for i := range skeleton.Feed {
		wg.Add(1)
		go func(ix int) {
			defer wg.Done()
			skeletonPost := skeleton.Feed[ix]
			postURI, err := syntax.ParseATURI(skeletonPost.Post)
			if err != nil {
				slog.Warn("invalid post URI in skeleton", "uri", skeletonPost.Post, "error", err)
				return
			}

			postInfo, err := hydrator.HydratePost(ctx, postURI.String(), viewer)
			if err != nil {
				if strings.Contains(err.Error(), "post not found") {
					hydrator.AddMissingRecord(postURI.String(), true)
					postInfo, err = hydrator.HydratePost(ctx, postURI.String(), viewer)
					if err != nil {
						slog.Error("failed to hydrate post after fetch missing", "uri", postURI, "error", err)
						return
					}
				} else {
					slog.Warn("failed to hydrate post", "uri", postURI, "error", err)
					return
				}
			}

			authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
			if err != nil {
				slog.Warn("failed to hydrate author", "did", postInfo.Author, "error", err)
				return
			}

			posts[ix] = views.FeedViewPost(postInfo, authorInfo)
		}(i)
	}
	wg.Wait()

	output := &bsky.FeedGetFeed_Output{
		Feed:   posts,
		Cursor: skeleton.Cursor,
	}

	return c.JSON(http.StatusOK, output)
}

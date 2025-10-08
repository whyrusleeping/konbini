package feed

import (
	"bytes"
	"log/slog"
	"net/http"
	"strconv"

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
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
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
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
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
		hydrator.AddMissingFeedGenerator(feedURI)
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "NotFound",
			"message": "feed generator not found",
		})
	}

	// Decode the feed generator record to get the service DID
	var feedGenRecord bsky.FeedGenerator
	if err := feedGenRecord.UnmarshalCBOR(bytes.NewReader(feedGen.Raw)); err != nil {
		slog.Error("failed to decode feed generator record", "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to decode feed generator record",
		})
	}

	// Parse the service DID
	serviceDID, err := syntax.ParseDID(feedGenRecord.Did)
	if err != nil {
		slog.Error("invalid service DID in feed generator", "error", err, "did", feedGenRecord.Did)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "invalid service DID",
		})
	}

	// Resolve the service DID to get its endpoint
	serviceIdent, err := dir.LookupDID(ctx, serviceDID)
	if err != nil {
		slog.Error("failed to resolve service DID", "error", err, "did", serviceDID)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to resolve service endpoint",
		})
	}

	serviceEndpoint := serviceIdent.GetServiceEndpoint("bsky_fg")
	if serviceEndpoint == "" {
		slog.Error("service has no bsky_fg endpoint", "did", serviceDID)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "service has no endpoint",
		})
	}

	// Create XRPC client for the feed generator service
	client := &xrpc.Client{
		Host: serviceEndpoint,
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
	for _, skeletonPost := range skeleton.Feed {
		postURI, err := syntax.ParseATURI(skeletonPost.Post)
		if err != nil {
			slog.Warn("invalid post URI in skeleton", "uri", skeletonPost.Post, "error", err)
			continue
		}

		postInfo, err := hydrator.HydratePost(ctx, string(postURI), viewer)
		if err != nil {
			slog.Warn("failed to hydrate post", "uri", postURI, "error", err)
			continue
		}

		authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
		if err != nil {
			slog.Warn("failed to hydrate author", "did", postInfo.Author, "error", err)
			continue
		}

		posts = append(posts, views.FeedViewPost(postInfo, authorInfo))
	}

	output := &bsky.FeedGetFeed_Output{
		Feed:   posts,
		Cursor: skeleton.Cursor,
	}

	return c.JSON(http.StatusOK, output)
}

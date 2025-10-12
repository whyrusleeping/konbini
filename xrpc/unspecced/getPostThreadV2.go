package unspecced

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetPostThreadV2 implements app.bsky.unspecced.getPostThreadV2
func HandleGetPostThreadV2(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	ctx := c.Request().Context()
	ctx = context.WithValue(ctx, "auto-fetch", true)

	// Parse parameters
	anchorRaw := c.QueryParam("anchor")
	if anchorRaw == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "anchor parameter is required",
		})
	}

	anchorUri, err := hydrator.NormalizeUri(ctx, anchorRaw)
	if err != nil {
		return err
	}

	// Parse optional parameters with defaults
	above := c.QueryParam("above") != "false" // default true

	below := int64(6) // default
	if belowParam := c.QueryParam("below"); belowParam != "" {
		if b, err := strconv.ParseInt(belowParam, 10, 64); err == nil && b >= 0 && b <= 20 {
			below = b
		}
	}

	branchingFactor := int64(10) // default
	if bfParam := c.QueryParam("branchingFactor"); bfParam != "" {
		if bf, err := strconv.ParseInt(bfParam, 10, 64); err == nil && bf > 0 {
			branchingFactor = bf
		}
	}

	_ = c.QueryParam("prioritizeFollowedUsers") == "true" // TODO: implement prioritization

	sort := c.QueryParam("sort")
	if sort == "" {
		sort = "newest"
	}

	viewer := getUserDID(c)

	// Hydrate the anchor post
	anchorPostInfo, err := hydrator.HydratePost(ctx, anchorUri, viewer)
	if err != nil {
		slog.Error("failed to hydrate post", "error", err, "anchor", anchorUri)
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "NotFound",
			"message": "anchor post not found",
		})
	}

	// Determine the root post ID for the thread
	rootPostID := anchorPostInfo.InThread
	if rootPostID == 0 {
		// This post is the root - get its ID
		var postID uint
		db.Raw(`
			SELECT id FROM posts
			WHERE author = (SELECT id FROM repos WHERE did = ?)
			AND rkey = ?
		`, extractDIDFromURI(anchorUri), extractRkeyFromURI(anchorUri)).Scan(&postID)
		rootPostID = postID
	}

	// Query all posts in this thread
	type threadPostRow struct {
		ID        uint
		Rkey      string
		ReplyTo   uint
		InThread  uint
		AuthorDid string
	}
	var threadPosts []threadPostRow
	db.Raw(`
		SELECT p.id, p.rkey, p.reply_to, p.in_thread, r.did as author_did
		FROM posts p
		JOIN repos r ON r.id = p.author
		WHERE (p.id = ? OR p.in_thread = ?)
		AND p.not_found = false
		ORDER BY p.created ASC
	`, rootPostID, rootPostID).Scan(&threadPosts)

	// Build a map of posts by ID
	postsByID := make(map[uint]*threadNode)
	for _, tp := range threadPosts {
		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", tp.AuthorDid, tp.Rkey)
		postsByID[tp.ID] = &threadNode{
			id:       tp.ID,
			uri:      uri,
			replyTo:  tp.ReplyTo,
			inThread: tp.InThread,
			children: []*threadNode{},
		}
	}

	// Build parent-child relationships
	for _, node := range postsByID {
		if node.replyTo != 0 {
			parent := postsByID[node.replyTo]
			if parent != nil {
				parent.children = append(parent.children, node)
			}
		}
	}

	// Find the anchor node
	anchorID := uint(0)
	for id, node := range postsByID {
		if node.uri == anchorUri {
			anchorID = id
			break
		}
	}

	if anchorID == 0 {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "NotFound",
			"message": "anchor post not found in thread",
		})
	}

	anchorNode := postsByID[anchorID]

	// Build flat thread items list
	var threadItems []*bsky.UnspeccedGetPostThreadV2_ThreadItem
	hasOtherReplies := false

	// Add parents if requested
	if above {
		parents := collectParents(anchorNode, postsByID)
		for i := len(parents) - 1; i >= 0; i-- {
			depth := int64(-(len(parents) - i))
			item := buildThreadItem(ctx, hydrator, parents[i], depth, viewer)
			if item != nil {
				threadItems = append(threadItems, item)
			}
		}
	}

	// Add anchor post (depth 0)
	anchorItem := buildThreadItem(ctx, hydrator, anchorNode, 0, viewer)
	if anchorItem != nil {
		threadItems = append(threadItems, anchorItem)
	}

	// Add replies below anchor
	if below > 0 {
		replies, hasMore := collectReplies(ctx, hydrator, anchorNode, 1, below, branchingFactor, sort, viewer)
		threadItems = append(threadItems, replies...)
		hasOtherReplies = hasMore
	}

	return c.JSON(http.StatusOK, &bsky.UnspeccedGetPostThreadV2_Output{
		Thread:          threadItems,
		HasOtherReplies: hasOtherReplies,
	})
}

type threadNode struct {
	id       uint
	uri      string
	replyTo  uint
	inThread uint
	children []*threadNode
}

func collectParents(node *threadNode, allNodes map[uint]*threadNode) []*threadNode {
	var parents []*threadNode
	current := node
	for current.replyTo != 0 {
		parent := allNodes[current.replyTo]
		if parent == nil {
			break
		}
		parents = append(parents, parent)
		current = parent
	}
	return parents
}

func collectReplies(ctx context.Context, hydrator *hydration.Hydrator, node *threadNode, currentDepth, maxDepth, branchingFactor int64, sort string, viewer string) ([]*bsky.UnspeccedGetPostThreadV2_ThreadItem, bool) {
	var items []*bsky.UnspeccedGetPostThreadV2_ThreadItem
	hasMore := false

	if currentDepth > maxDepth {
		return items, false
	}

	// Sort children based on sort parameter
	children := node.children
	// TODO: Actually sort based on the sort parameter (newest/oldest/top)
	// For now, just use the order we have

	// Limit to branchingFactor
	limit := int(branchingFactor)
	if len(children) > limit {
		hasMore = true
		children = children[:limit]
	}

	for _, child := range children {
		item := buildThreadItem(ctx, hydrator, child, currentDepth, viewer)
		if item != nil {
			items = append(items, item)

			// Recursively collect replies
			if currentDepth < maxDepth {
				childReplies, childHasMore := collectReplies(ctx, hydrator, child, currentDepth+1, maxDepth, branchingFactor, sort, viewer)
				items = append(items, childReplies...)
				if childHasMore {
					hasMore = true
				}
			}
		}
	}

	return items, hasMore
}

func buildThreadItem(ctx context.Context, hydrator *hydration.Hydrator, node *threadNode, depth int64, viewer string) *bsky.UnspeccedGetPostThreadV2_ThreadItem {
	// Hydrate the post
	postInfo, err := hydrator.HydratePost(ctx, node.uri, viewer)
	if err != nil {
		// Return not found item
		return &bsky.UnspeccedGetPostThreadV2_ThreadItem{
			Depth: depth,
			Uri:   node.uri,
			Value: &bsky.UnspeccedGetPostThreadV2_ThreadItem_Value{
				UnspeccedDefs_ThreadItemNotFound: &bsky.UnspeccedDefs_ThreadItemNotFound{
					LexiconTypeID: "app.bsky.unspecced.defs#threadItemNotFound",
				},
			},
		}
	}

	// Hydrate author
	authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
	if err != nil {
		return &bsky.UnspeccedGetPostThreadV2_ThreadItem{
			Depth: depth,
			Uri:   node.uri,
			Value: &bsky.UnspeccedGetPostThreadV2_ThreadItem_Value{
				UnspeccedDefs_ThreadItemNotFound: &bsky.UnspeccedDefs_ThreadItemNotFound{
					LexiconTypeID: "app.bsky.unspecced.defs#threadItemNotFound",
				},
			},
		}
	}

	// Build post view
	postView := views.PostView(postInfo, authorInfo)

	// Calculate moreReplies count
	moreReplies := int64(0)
	if len(node.children) > 0 {
		// This is a simplified calculation - actual count would need more complex logic
		moreReplies = int64(len(node.children))
	}

	return &bsky.UnspeccedGetPostThreadV2_ThreadItem{
		Depth: depth,
		Uri:   node.uri,
		Value: &bsky.UnspeccedGetPostThreadV2_ThreadItem_Value{
			UnspeccedDefs_ThreadItemPost: &bsky.UnspeccedDefs_ThreadItemPost{
				LexiconTypeID:      "app.bsky.unspecced.defs#threadItemPost",
				Post:               postView,
				HiddenByThreadgate: false,
				MoreParents:        false,
				MoreReplies:        moreReplies,
				MutedByViewer:      false,
				OpThread:           false, // TODO: Calculate this properly
			},
		},
	}
}

func getUserDID(c echo.Context) string {
	did := c.Get("viewer")
	if did == nil {
		return ""
	}
	if s, ok := did.(string); ok {
		return s
	}
	return ""
}

func extractDIDFromURI(uri string) string {
	// URI format: at://did:plc:xxx/collection/rkey
	if len(uri) < 5 || uri[:5] != "at://" {
		return ""
	}
	parts := []rune(uri[5:])
	for i, r := range parts {
		if r == '/' {
			return string(parts[:i])
		}
	}
	return string(parts)
}

func extractRkeyFromURI(uri string) string {
	// URI format: at://did:plc:xxx/collection/rkey
	if len(uri) < 5 || uri[:5] != "at://" {
		return ""
	}
	// Find last slash
	for i := len(uri) - 1; i >= 5; i-- {
		if uri[i] == '/' {
			return uri[i+1:]
		}
	}
	return ""
}

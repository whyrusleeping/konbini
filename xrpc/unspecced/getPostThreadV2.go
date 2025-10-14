package unspecced

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"github.com/whyrusleeping/market/models"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
)

var tracer = otel.Tracer("xrpc/unspecced")

// HandleGetPostThreadV2 implements app.bsky.unspecced.getPostThreadV2
func HandleGetPostThreadV2(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	ctx, span := tracer.Start(c.Request().Context(), "getPostThreadV2")
	defer span.End()
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

	threadID := anchorPostInfo.InThread
	if threadID == 0 {
		threadID = anchorPostInfo.ID
	}

	var threadPosts []*models.Post
	if err := db.Raw("SELECT * FROM posts WHERE in_thread = ? OR id = ?", threadID, anchorPostInfo.ID).Scan(&threadPosts).Error; err != nil {
		return err
	}

	fmt.Println("GOT THREAD POSTS: ", len(threadPosts))

	treeNodes, err := buildThreadTree(ctx, hydrator, db, threadPosts)
	if err != nil {
		return fmt.Errorf("failed to construct tree: %w", err)
	}

	anchor := treeNodes[anchorPostInfo.ID]

	// Build flat thread items list
	var threadItems []*bsky.UnspeccedGetPostThreadV2_ThreadItem
	hasOtherReplies := false

	// Add parents if requested
	if above {
		parent := anchor.parent
		depth := int64(-1)
		for parent != nil {
			if parent.missing {
				fmt.Println("Parent missing: ", depth)
				item := &bsky.UnspeccedGetPostThreadV2_ThreadItem{
					Depth: depth,
					Uri:   parent.uri,
					Value: &bsky.UnspeccedGetPostThreadV2_ThreadItem_Value{
						UnspeccedDefs_ThreadItemNotFound: &bsky.UnspeccedDefs_ThreadItemNotFound{
							LexiconTypeID: "app.bsky.unspecced.defs#threadItemNotFound",
						},
					},
				}

				threadItems = append(threadItems, item)
				break
			}

			item := buildThreadItem(ctx, hydrator, parent, depth, viewer)
			if item != nil {
				threadItems = append(threadItems, item)
			}

			parent = parent.parent
			depth--
		}
	}

	// Add anchor post (depth 0)
	anchorItem := buildThreadItem(ctx, hydrator, anchor, 0, viewer)
	if anchorItem != nil {
		threadItems = append(threadItems, anchorItem)
	}

	// Add replies below anchor
	if below > 0 {
		replies, err := collectReplies(ctx, hydrator, anchor, 0, below, branchingFactor, sort, viewer)
		if err != nil {
			return err
		}
		threadItems = append(threadItems, replies...)
		//hasOtherReplies = hasMore
	}

	return c.JSON(http.StatusOK, &bsky.UnspeccedGetPostThreadV2_Output{
		Thread:          threadItems,
		HasOtherReplies: hasOtherReplies,
	})
}

func collectReplies(ctx context.Context, hydrator *hydration.Hydrator, curnode *threadTree, depth int64, below int64, branchingFactor int64, sort string, viewer string) ([]*bsky.UnspeccedGetPostThreadV2_ThreadItem, error) {
	if below == 0 {
		return nil, nil
	}

	type parThreadResults struct {
		node     *bsky.UnspeccedGetPostThreadV2_ThreadItem
		children []*bsky.UnspeccedGetPostThreadV2_ThreadItem
	}

	results := make([]parThreadResults, len(curnode.children))

	var wg sync.WaitGroup
	for i := range curnode.children {
		ix := i
		wg.Go(func() {
			child := curnode.children[ix]

			results[ix].node = buildThreadItem(ctx, hydrator, child, depth+1, viewer)
			if child.missing {
				return
			}

			sub, err := collectReplies(ctx, hydrator, child, depth+1, below-1, branchingFactor, sort, viewer)
			if err != nil {
				slog.Error("failed to collect replies", "node", child.uri, "error", err)
				return
			}

			results[ix].children = sub
		})
	}

	wg.Wait()

	var out []*bsky.UnspeccedGetPostThreadV2_ThreadItem
	for _, res := range results {
		out = append(out, res.node)
		out = append(out, res.children...)
	}

	return out, nil
}

func buildThreadItem(ctx context.Context, hydrator *hydration.Hydrator, node *threadTree, depth int64, viewer string) *bsky.UnspeccedGetPostThreadV2_ThreadItem {
	if node.missing {
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

	// Hydrate the post
	postInfo, err := hydrator.HydratePostDB(ctx, node.uri, node.val, viewer)
	if err != nil {
		slog.Error("failed to hydrate post in thread item", "uri", node.uri, "error", err)
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
		slog.Error("failed to hydrate actor in thread item", "author", postInfo.Author, "error", err)
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

type threadTree struct {
	parent   *threadTree
	children []*threadTree

	val *models.Post

	missing bool

	uri string
	cid string
}

func buildThreadTree(ctx context.Context, hydrator *hydration.Hydrator, db *gorm.DB, posts []*models.Post) (map[uint]*threadTree, error) {
	nodes := make(map[uint]*threadTree)
	for _, p := range posts {
		puri, err := hydrator.UriForPost(ctx, p)
		if err != nil {
			return nil, err
		}

		t := &threadTree{
			val: p,
			uri: puri,
		}

		nodes[p.ID] = t
	}

	missing := make(map[uint]*threadTree)
	for _, node := range nodes {
		if node.val.ReplyTo == 0 {
			continue
		}

		pnode, ok := nodes[node.val.ReplyTo]
		if !ok {
			pnode = &threadTree{
				missing: true,
			}
			missing[node.val.ReplyTo] = pnode

			var bspost bsky.FeedPost
			if err := bspost.UnmarshalCBOR(bytes.NewReader(node.val.Raw)); err != nil {
				return nil, err
			}

			if bspost.Reply == nil || bspost.Reply.Parent == nil {
				return nil, fmt.Errorf("node with parent had no parent in object")
			}

			pnode.uri = bspost.Reply.Parent.Uri
			pnode.cid = bspost.Reply.Parent.Cid

			/* Maybe we could force hydrate these?
			hydrator.AddMissingRecord(puri, true)
			*/
		}

		pnode.children = append(pnode.children, node)
		node.parent = pnode
	}

	for k, v := range missing {
		nodes[k] = v
	}

	return nodes, nil
}

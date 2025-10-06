package feed

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetPostThread implements app.bsky.feed.getPostThread
func HandleGetPostThread(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	uriParam := c.QueryParam("uri")
	if uriParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "uri parameter is required",
		})
	}

	ctx := c.Request().Context()
	viewer := getUserDID(c)

	// Hydrate the requested post
	postInfo, err := hydrator.HydratePost(ctx, uriParam, viewer)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "NotFound",
			"message": "post not found",
		})
	}

	// Determine the root post ID for the thread
	rootPostID := postInfo.InThread
	if rootPostID == 0 {
		// This post is the root
		// Query to find what the post's internal ID is
		var postID uint
		db.Raw(`
			SELECT id FROM posts
			WHERE author = (SELECT id FROM repos WHERE did = ?)
			AND rkey = ?
		`, extractDIDFromURI(uriParam), extractRkeyFromURI(uriParam)).Scan(&postID)
		rootPostID = postID
	}

	// Query all posts in this thread
	type threadPost struct {
		ID        uint
		Rkey      string
		ReplyTo   uint
		InThread  uint
		AuthorDID string
	}
	var threadPosts []threadPost
	db.Raw(`
		SELECT p.id, p.rkey, p.reply_to, p.in_thread, r.did as author_did
		FROM posts p
		JOIN repos r ON r.id = p.author
		WHERE (p.id = ? OR p.in_thread = ?)
		AND p.not_found = false
		ORDER BY p.created ASC
	`, rootPostID, rootPostID).Scan(&threadPosts)

	// Build a map of posts by ID for easy lookup
	postsByID := make(map[uint]*threadPostNode)
	for _, tp := range threadPosts {
		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", tp.AuthorDID, tp.Rkey)
		postsByID[tp.ID] = &threadPostNode{
			id:       tp.ID,
			uri:      uri,
			replyTo:  tp.ReplyTo,
			inThread: tp.InThread,
			replies:  []interface{}{},
		}
	}

	// Build the thread tree structure
	for _, node := range postsByID {
		if node.replyTo != 0 {
			parent := postsByID[node.replyTo]
			if parent != nil {
				parent.replies = append(parent.replies, node)
			}
		}
	}

	// Find the root node
	var rootNode *threadPostNode
	for _, node := range postsByID {
		if node.inThread == 0 || node.id == rootPostID {
			rootNode = node
			break
		}
	}

	if rootNode == nil {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "NotFound",
			"message": "thread root not found",
		})
	}

	// Build the response by traversing the tree
	thread := buildThreadView(ctx, db, rootNode, postsByID, hydrator, viewer, nil)

	return c.JSON(http.StatusOK, map[string]interface{}{
		"thread": thread,
	})
}

type threadPostNode struct {
	id       uint
	uri      string
	replyTo  uint
	inThread uint
	replies  []interface{}
}

func buildThreadView(ctx context.Context, db *gorm.DB, node *threadPostNode, allNodes map[uint]*threadPostNode, hydrator *hydration.Hydrator, viewer string, parent interface{}) interface{} {
	// Hydrate this post
	postInfo, err := hydrator.HydratePost(ctx, node.uri, viewer)
	if err != nil {
		// Return a notFound post
		return map[string]interface{}{
			"$type": "app.bsky.feed.defs#notFoundPost",
			"uri":   node.uri,
		}
	}

	// Hydrate author
	authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
	if err != nil {
		return map[string]interface{}{
			"$type": "app.bsky.feed.defs#notFoundPost",
			"uri":   node.uri,
		}
	}

	// Build replies
	var replies []interface{}
	for _, replyNode := range node.replies {
		if rn, ok := replyNode.(*threadPostNode); ok {
			replyView := buildThreadView(ctx, db, rn, allNodes, hydrator, viewer, nil)
			replies = append(replies, replyView)
		}
	}

	// Build the thread view post
	var repliesForView interface{}
	if len(replies) > 0 {
		repliesForView = replies
	}

	return views.ThreadViewPost(postInfo, authorInfo, parent, repliesForView)
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

package hydration

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/indigo/api/bsky"
)

// PostInfo contains hydrated post information
type PostInfo struct {
	URI         string
	Cid         string
	Post        *bsky.FeedPost
	Author      string // DID
	ReplyTo     uint
	ReplyToUsr  uint
	InThread    uint
	LikeCount   int
	RepostCount int
	ReplyCount  int
	ViewerLike  string // URI of viewer's like, if any
}

const fakeCid = "bafyreiapw4hagb5ehqgoeho4v23vf7fhlqey4b7xvjpy76krgkqx7xlolu"

// HydratePost hydrates a single post by URI
func (h *Hydrator) HydratePost(ctx context.Context, uri string, viewerDID string) (*PostInfo, error) {
	// Query post from database
	var dbPost struct {
		ID         uint
		Cid        string
		Raw        []byte
		NotFound   bool
		ReplyTo    uint
		ReplyToUsr uint
		InThread   uint
		AuthorID   uint
	}

	err := h.db.Raw(`
		SELECT p.id, p.cid, p.raw, p.not_found, p.reply_to, p.reply_to_usr, p.in_thread, p.author as author_id
		FROM posts p
		WHERE p.id = (
			SELECT id FROM posts
			WHERE author = (SELECT id FROM repos WHERE did = ?)
			AND rkey = ?
		)
	`, extractDIDFromURI(uri), extractRkeyFromURI(uri)).Scan(&dbPost).Error

	if err != nil {
		return nil, fmt.Errorf("failed to query post: %w", err)
	}

	if dbPost.NotFound || len(dbPost.Raw) == 0 {
		return nil, fmt.Errorf("post not found")
	}

	// Unmarshal post record
	var feedPost bsky.FeedPost
	if err := feedPost.UnmarshalCBOR(bytes.NewReader(dbPost.Raw)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal post: %w", err)
	}

	// Get author DID
	var authorDID string
	h.db.Raw("SELECT did FROM repos WHERE id = ?", dbPost.AuthorID).Scan(&authorDID)

	// Get engagement counts
	var likes, reposts, replies int
	h.db.Raw("SELECT COUNT(*) FROM likes WHERE subject = ?", dbPost.ID).Scan(&likes)
	h.db.Raw("SELECT COUNT(*) FROM reposts WHERE subject = ?", dbPost.ID).Scan(&reposts)
	h.db.Raw("SELECT COUNT(*) FROM posts WHERE reply_to = ?", dbPost.ID).Scan(&replies)

	info := &PostInfo{
		URI:         uri,
		Cid:         dbPost.Cid,
		Post:        &feedPost,
		Author:      authorDID,
		ReplyTo:     dbPost.ReplyTo,
		ReplyToUsr:  dbPost.ReplyToUsr,
		InThread:    dbPost.InThread,
		LikeCount:   likes,
		RepostCount: reposts,
		ReplyCount:  replies,
	}

	if info.Cid == "" {
		slog.Error("MISSING CID", "uri", uri)
		info.Cid = fakeCid
	}

	// Check if viewer liked this post
	if viewerDID != "" {
		var likeRkey string
		h.db.Raw(`
			SELECT l.rkey FROM likes l
			WHERE l.subject = ?
			AND l.author = (SELECT id FROM repos WHERE did = ?)
		`, dbPost.ID, viewerDID).Scan(&likeRkey)
		if likeRkey != "" {
			info.ViewerLike = fmt.Sprintf("at://%s/app.bsky.feed.like/%s", viewerDID, likeRkey)
		}
	}

	return info, nil
}

// HydratePosts hydrates multiple posts
func (h *Hydrator) HydratePosts(ctx context.Context, uris []string, viewerDID string) (map[string]*PostInfo, error) {
	result := make(map[string]*PostInfo, len(uris))
	for _, uri := range uris {
		info, err := h.HydratePost(ctx, uri, viewerDID)
		if err != nil {
			// Skip posts that fail to hydrate
			continue
		}
		result[uri] = info
	}
	return result, nil
}

// Helper functions to extract DID and rkey from AT URI
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

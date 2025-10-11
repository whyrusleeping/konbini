package views

import (
	"fmt"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/whyrusleeping/konbini/hydration"
)

// PostView builds a post view (app.bsky.feed.defs#postView)
func PostView(post *hydration.PostInfo, author *hydration.ActorInfo) *bsky.FeedDefs_PostView {
	view := &bsky.FeedDefs_PostView{
		LexiconTypeID: "app.bsky.feed.defs#postView",
		Uri:           post.URI,
		Cid:           post.Cid,
		Author:        ProfileViewBasic(author),
		Record: &util.LexiconTypeDecoder{
			Val: post.Post,
		},
		IndexedAt: post.Post.CreatedAt, // Using createdAt as indexedAt for now
	}

	// Add engagement counts
	if post.LikeCount > 0 {
		lc := int64(post.LikeCount)
		view.LikeCount = &lc
	}
	if post.RepostCount > 0 {
		rc := int64(post.RepostCount)
		view.RepostCount = &rc
	}
	if post.ReplyCount > 0 {
		rpc := int64(post.ReplyCount)
		view.ReplyCount = &rpc
	}

	// Add viewer state
	if post.ViewerLike != "" {
		view.Viewer = &bsky.FeedDefs_ViewerState{
			Like: &post.ViewerLike,
		}
	}

	// Add embed if it was hydrated
	if post.EmbedInfo != nil {
		view.Embed = post.EmbedInfo
	}

	return view
}

// FeedViewPost builds a feed view post (app.bsky.feed.defs#feedViewPost)
func FeedViewPost(post *hydration.PostInfo, author *hydration.ActorInfo) *bsky.FeedDefs_FeedViewPost {
	return &bsky.FeedDefs_FeedViewPost{
		Post: PostView(post, author),
	}
}

// ThreadViewPost builds a thread view post (app.bsky.feed.defs#threadViewPost)
func ThreadViewPost(post *hydration.PostInfo, author *hydration.ActorInfo, parent, replies any) *bsky.FeedDefs_ThreadViewPost {
	view := &bsky.FeedDefs_ThreadViewPost{
		LexiconTypeID: "app.bsky.feed.defs#threadViewPost",
		Post:          PostView(post, author),
	}

	// TODO: Type parent and replies properly as union types
	// For now leaving them as interface{} to be handled by handlers

	return view
}

// GeneratorView builds a feed generator view (app.bsky.feed.defs#generatorView)
func GeneratorView(uri, cid string, record *bsky.FeedGenerator, creator *hydration.ActorInfo, likeCount int64, viewerLike string, indexedAt string) *bsky.FeedDefs_GeneratorView {
	view := &bsky.FeedDefs_GeneratorView{
		LexiconTypeID: "app.bsky.feed.defs#generatorView",
		Uri:           uri,
		Cid:           cid,
		Did:           record.Did,
		Creator:       ProfileView(creator),
		DisplayName:   record.DisplayName,
		Description:   record.Description,
		IndexedAt:     indexedAt,
	}

	// Add optional fields
	if record.Avatar != nil {
		avatarURL := fmt.Sprintf("https://cdn.bsky.app/img/avatar/plain/%s/%s@jpeg", creator.DID, record.Avatar.Ref.String())
		view.Avatar = &avatarURL
	}

	if record.DescriptionFacets != nil && len(record.DescriptionFacets) > 0 {
		view.DescriptionFacets = record.DescriptionFacets
	}

	if record.AcceptsInteractions != nil {
		view.AcceptsInteractions = record.AcceptsInteractions
	}

	if record.ContentMode != nil {
		view.ContentMode = record.ContentMode
	}

	// Add like count if present
	if likeCount > 0 {
		view.LikeCount = &likeCount
	}

	// Add viewer state if viewer has liked
	if viewerLike != "" {
		view.Viewer = &bsky.FeedDefs_GeneratorViewerState{
			Like: &viewerLike,
		}
	}

	return view
}

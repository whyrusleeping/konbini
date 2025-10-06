package views

import (
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

	// TODO: Add embed handling - need to convert embed types to proper views
	// if post.Post.Embed != nil {
	// 	view.Embed = formatEmbed(post.Post.Embed)
	// }

	return view
}

// FeedViewPost builds a feed view post (app.bsky.feed.defs#feedViewPost)
func FeedViewPost(post *hydration.PostInfo, author *hydration.ActorInfo) *bsky.FeedDefs_FeedViewPost {
	return &bsky.FeedDefs_FeedViewPost{
		Post: PostView(post, author),
	}
}

// ThreadViewPost builds a thread view post (app.bsky.feed.defs#threadViewPost)
func ThreadViewPost(post *hydration.PostInfo, author *hydration.ActorInfo, parent, replies interface{}) *bsky.FeedDefs_ThreadViewPost {
	view := &bsky.FeedDefs_ThreadViewPost{
		LexiconTypeID: "app.bsky.feed.defs#threadViewPost",
		Post:          PostView(post, author),
	}

	// TODO: Type parent and replies properly as union types
	// For now leaving them as interface{} to be handled by handlers

	return view
}

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

	// Add embed handling
	if post.Post.Embed != nil {
		view.Embed = formatEmbed(post.Post.Embed, post.Author)
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

func formatEmbed(embed *bsky.FeedPost_Embed, authorDID string) *bsky.FeedDefs_PostView_Embed {
	if embed == nil {
		return nil
	}

	result := &bsky.FeedDefs_PostView_Embed{}

	// Handle images
	if embed.EmbedImages != nil {
		viewImages := make([]*bsky.EmbedImages_ViewImage, len(embed.EmbedImages.Images))
		for i, img := range embed.EmbedImages.Images {
			// Convert blob to CDN URLs
			fullsize := ""
			thumb := ""
			if img.Image != nil {
				// CDN URL format for feed images
				cid := img.Image.Ref.String()
				fullsize = fmt.Sprintf("https://cdn.bsky.app/img/feed_fullsize/plain/%s/%s@jpeg", authorDID, cid)
				thumb = fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", authorDID, cid)
			}

			viewImages[i] = &bsky.EmbedImages_ViewImage{
				Alt:         img.Alt,
				AspectRatio: img.AspectRatio,
				Fullsize:    fullsize,
				Thumb:       thumb,
			}
		}
		result.EmbedImages_View = &bsky.EmbedImages_View{
			LexiconTypeID: "app.bsky.embed.images#view",
			Images:        viewImages,
		}
		return result
	}

	// Handle external links
	if embed.EmbedExternal != nil && embed.EmbedExternal.External != nil {
		// Convert blob thumb to CDN URL if present
		var thumbURL *string
		if embed.EmbedExternal.External.Thumb != nil {
			// CDN URL for external link thumbnails
			cid := embed.EmbedExternal.External.Thumb.Ref.String()
			url := fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", authorDID, cid)
			thumbURL = &url
		}

		result.EmbedExternal_View = &bsky.EmbedExternal_View{
			LexiconTypeID: "app.bsky.embed.external#view",
			External: &bsky.EmbedExternal_ViewExternal{
				Uri:         embed.EmbedExternal.External.Uri,
				Title:       embed.EmbedExternal.External.Title,
				Description: embed.EmbedExternal.External.Description,
				Thumb:       thumbURL,
			},
		}
		return result
	}

	// Handle video
	if embed.EmbedVideo != nil {
		// TODO: Implement video embed view
		// This would require converting video blob to CDN URLs and playlist URLs
		return nil
	}

	// Handle record (quote posts, etc.)
	if embed.EmbedRecord != nil {
		// TODO: Implement record embed view
		// This requires hydrating the embedded record, which is complex
		// For now, return nil to skip these embeds
		return nil
	}

	// Handle record with media (quote post with images/external)
	if embed.EmbedRecordWithMedia != nil {
		// TODO: Implement record with media embed view
		// This combines record hydration with media conversion
		return nil
	}

	return nil
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

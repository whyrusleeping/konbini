package hydration

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/whyrusleeping/market/models"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("hydrator")

// PostInfo contains hydrated post information
type PostInfo struct {
	ID          uint
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

	EmbedInfo *bsky.FeedDefs_PostView_Embed
}

const fakeCid = "bafyreiapw4hagb5ehqgoeho4v23vf7fhlqey4b7xvjpy76krgkqx7xlolu"

// HydratePost hydrates a single post by URI
func (h *Hydrator) HydratePost(ctx context.Context, uri string, viewerDID string) (*PostInfo, error) {
	ctx, span := tracer.Start(ctx, "hydratePost")
	defer span.End()

	p, err := h.backend.GetPostByUri(ctx, uri, "*")
	if err != nil {
		return nil, err
	}

	return h.HydratePostDB(ctx, uri, p, viewerDID)
}

func (h *Hydrator) HydratePostDB(ctx context.Context, uri string, dbPost *models.Post, viewerDID string) (*PostInfo, error) {
	autoFetch, _ := ctx.Value("auto-fetch").(bool)

	authorDid := extractDIDFromURI(uri)
	r, err := h.backend.GetOrCreateRepo(ctx, authorDid)
	if err != nil {
		return nil, err
	}

	if dbPost.NotFound || len(dbPost.Raw) == 0 {
		if autoFetch {
			h.AddMissingRecord(uri, true)
			if err := h.db.Raw(`SELECT * FROM posts WHERE author = ? AND rkey = ? `, r.ID, extractRkeyFromURI(uri)).Scan(&dbPost).Error; err != nil {
				return nil, fmt.Errorf("failed to query post: %w", err)
			}
			if dbPost.NotFound || len(dbPost.Raw) == 0 {
				return nil, fmt.Errorf("post not found")
			}
		} else {
			return nil, fmt.Errorf("post not found")
		}
	}

	// Unmarshal post record
	var feedPost bsky.FeedPost
	if err := feedPost.UnmarshalCBOR(bytes.NewReader(dbPost.Raw)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal post: %w", err)
	}

	var wg sync.WaitGroup

	authorDID := r.Did

	// Get engagement counts
	var likes, reposts, replies int
	wg.Go(func() {
		_, span := tracer.Start(ctx, "likeCounts")
		defer span.End()
		h.db.Raw("SELECT COUNT(*) FROM likes WHERE subject = ?", dbPost.ID).Scan(&likes)
	})
	wg.Go(func() {
		_, span := tracer.Start(ctx, "repostCounts")
		defer span.End()
		h.db.Raw("SELECT COUNT(*) FROM reposts WHERE subject = ?", dbPost.ID).Scan(&reposts)
	})
	wg.Go(func() {
		_, span := tracer.Start(ctx, "replyCounts")
		defer span.End()
		h.db.Raw("SELECT COUNT(*) FROM posts WHERE reply_to = ?", dbPost.ID).Scan(&replies)
	})

	// Check if viewer liked this post
	var likeRkey string
	if viewerDID != "" {
		wg.Go(func() {
			_, span := tracer.Start(ctx, "viewerLikeState")
			defer span.End()
			h.db.Raw(`
			SELECT l.rkey FROM likes l
			WHERE l.subject = ?
			AND l.author = (SELECT id FROM repos WHERE did = ?)
		`, dbPost.ID, viewerDID).Scan(&likeRkey)
		})
	}

	var ei *bsky.FeedDefs_PostView_Embed
	if feedPost.Embed != nil {
		wg.Go(func() {
			ei = h.formatEmbed(ctx, feedPost.Embed, authorDID, viewerDID)
		})
	}

	wg.Wait()

	info := &PostInfo{
		ID:          dbPost.ID,
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
		EmbedInfo:   ei,
	}

	if likeRkey != "" {
		info.ViewerLike = fmt.Sprintf("at://%s/app.bsky.feed.like/%s", viewerDID, likeRkey)
	}

	if info.Cid == "" {
		slog.Error("MISSING CID", "uri", uri)
		info.Cid = fakeCid
	}

	// Hydrate embed

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

func (h *Hydrator) formatEmbed(ctx context.Context, embed *bsky.FeedPost_Embed, authorDID string, viewerDID string) *bsky.FeedDefs_PostView_Embed {
	if embed == nil {
		return nil
	}
	_, span := tracer.Start(ctx, "formatEmbed")
	defer span.End()

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
	if embed.EmbedVideo != nil && embed.EmbedVideo.Video != nil {
		cid := embed.EmbedVideo.Video.Ref.String()
		// URL-encode the DID (replace : with %3A)
		encodedDID := ""
		for _, ch := range authorDID {
			if ch == ':' {
				encodedDID += "%3A"
			} else {
				encodedDID += string(ch)
			}
		}

		playlist := fmt.Sprintf("https://video.bsky.app/watch/%s/%s/playlist.m3u8", encodedDID, cid)
		thumbnail := fmt.Sprintf("https://video.bsky.app/watch/%s/%s/thumbnail.jpg", encodedDID, cid)

		result.EmbedVideo_View = &bsky.EmbedVideo_View{
			LexiconTypeID: "app.bsky.embed.video#view",
			Cid:           cid,
			Playlist:      playlist,
			Thumbnail:     &thumbnail,
			Alt:           embed.EmbedVideo.Alt,
			AspectRatio:   embed.EmbedVideo.AspectRatio,
		}
		return result
	}

	// Handle record (quote posts, etc.)
	if embed.EmbedRecord != nil && embed.EmbedRecord.Record != nil {
		rec := embed.EmbedRecord.Record

		result.EmbedRecord_View = &bsky.EmbedRecord_View{
			LexiconTypeID: "app.bsky.embed.record#view",
			Record:        h.hydrateEmbeddedRecord(ctx, rec.Uri, viewerDID),
		}
		return result
	}

	// Handle record with media (quote post with images/external)
	if embed.EmbedRecordWithMedia != nil {
		recordView := &bsky.EmbedRecordWithMedia_View{
			LexiconTypeID: "app.bsky.embed.recordWithMedia#view",
		}

		// Hydrate the record part
		if embed.EmbedRecordWithMedia.Record != nil && embed.EmbedRecordWithMedia.Record.Record != nil {
			recordView.Record = &bsky.EmbedRecord_View{
				LexiconTypeID: "app.bsky.embed.record#view",
				Record:        h.hydrateEmbeddedRecord(ctx, embed.EmbedRecordWithMedia.Record.Record.Uri, viewerDID),
			}
		}

		// Hydrate the media part (images or external)
		if embed.EmbedRecordWithMedia.Media != nil {
			if embed.EmbedRecordWithMedia.Media.EmbedImages != nil {
				viewImages := make([]*bsky.EmbedImages_ViewImage, len(embed.EmbedRecordWithMedia.Media.EmbedImages.Images))
				for i, img := range embed.EmbedRecordWithMedia.Media.EmbedImages.Images {
					fullsize := ""
					thumb := ""
					if img.Image != nil {
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
				recordView.Media = &bsky.EmbedRecordWithMedia_View_Media{
					EmbedImages_View: &bsky.EmbedImages_View{
						LexiconTypeID: "app.bsky.embed.images#view",
						Images:        viewImages,
					},
				}
			} else if embed.EmbedRecordWithMedia.Media.EmbedExternal != nil && embed.EmbedRecordWithMedia.Media.EmbedExternal.External != nil {
				var thumbURL *string
				if embed.EmbedRecordWithMedia.Media.EmbedExternal.External.Thumb != nil {
					cid := embed.EmbedRecordWithMedia.Media.EmbedExternal.External.Thumb.Ref.String()
					url := fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", authorDID, cid)
					thumbURL = &url
				}

				recordView.Media = &bsky.EmbedRecordWithMedia_View_Media{
					EmbedExternal_View: &bsky.EmbedExternal_View{
						LexiconTypeID: "app.bsky.embed.external#view",
						External: &bsky.EmbedExternal_ViewExternal{
							Uri:         embed.EmbedRecordWithMedia.Media.EmbedExternal.External.Uri,
							Title:       embed.EmbedRecordWithMedia.Media.EmbedExternal.External.Title,
							Description: embed.EmbedRecordWithMedia.Media.EmbedExternal.External.Description,
							Thumb:       thumbURL,
						},
					},
				}
			} else if embed.EmbedRecordWithMedia.Media.EmbedVideo != nil && embed.EmbedRecordWithMedia.Media.EmbedVideo.Video != nil {
				cid := embed.EmbedRecordWithMedia.Media.EmbedVideo.Video.Ref.String()
				// URL-encode the DID (replace : with %3A)
				encodedDID := ""
				for _, ch := range authorDID {
					if ch == ':' {
						encodedDID += "%3A"
					} else {
						encodedDID += string(ch)
					}
				}

				playlist := fmt.Sprintf("https://video.bsky.app/watch/%s/%s/playlist.m3u8", encodedDID, cid)
				thumbnail := fmt.Sprintf("https://video.bsky.app/watch/%s/%s/thumbnail.jpg", encodedDID, cid)

				recordView.Media = &bsky.EmbedRecordWithMedia_View_Media{
					EmbedVideo_View: &bsky.EmbedVideo_View{
						LexiconTypeID: "app.bsky.embed.video#view",
						Cid:           cid,
						Playlist:      playlist,
						Thumbnail:     &thumbnail,
						Alt:           embed.EmbedRecordWithMedia.Media.EmbedVideo.Alt,
						AspectRatio:   embed.EmbedRecordWithMedia.Media.EmbedVideo.AspectRatio,
					},
				}
			}
		}

		result.EmbedRecordWithMedia_View = recordView
		return result
	}

	return nil
}

// hydrateEmbeddedRecord hydrates an embedded record (for quote posts, etc.)
func (h *Hydrator) hydrateEmbeddedRecord(ctx context.Context, uri string, viewerDID string) *bsky.EmbedRecord_View_Record {
	ctx, span := tracer.Start(ctx, "hydrateEmbeddedRecord")
	defer span.End()

	// Check if it's a post URI
	if !isPostURI(uri) {
		// Could be a feed generator, list, labeler, or starter pack
		// For now, return not found for non-post embeds
		return &bsky.EmbedRecord_View_Record{
			EmbedRecord_ViewNotFound: &bsky.EmbedRecord_ViewNotFound{
				LexiconTypeID: "app.bsky.embed.record#viewNotFound",
				Uri:           uri,
			},
		}
	}

	// Try to hydrate the post
	quotedPost, err := h.HydratePost(ctx, uri, viewerDID)
	if err != nil {
		// Post not found
		return &bsky.EmbedRecord_View_Record{
			EmbedRecord_ViewNotFound: &bsky.EmbedRecord_ViewNotFound{
				LexiconTypeID: "app.bsky.embed.record#viewNotFound",
				Uri:           uri,
				NotFound:      true,
			},
		}
	}

	// Hydrate the author
	authorInfo, err := h.HydrateActor(ctx, quotedPost.Author)
	if err != nil {
		// Author not found, treat as not found
		return &bsky.EmbedRecord_View_Record{
			EmbedRecord_ViewNotFound: &bsky.EmbedRecord_ViewNotFound{
				LexiconTypeID: "app.bsky.embed.record#viewNotFound",
				Uri:           uri,
				NotFound:      true,
			},
		}
	}

	// TODO: Check if viewer has blocked or is blocked by the author
	// For now, just return the record view

	// Build the author profile view
	authorView := &bsky.ActorDefs_ProfileViewBasic{
		Did:    authorInfo.DID,
		Handle: authorInfo.Handle,
	}
	if authorInfo.Profile != nil {
		if authorInfo.Profile.DisplayName != nil && *authorInfo.Profile.DisplayName != "" {
			authorView.DisplayName = authorInfo.Profile.DisplayName
		}
		if authorInfo.Profile.Avatar != nil {
			avatarURL := fmt.Sprintf("https://cdn.bsky.app/img/avatar_thumbnail/plain/%s/%s@jpeg", authorInfo.DID, authorInfo.Profile.Avatar.Ref.String())
			authorView.Avatar = &avatarURL
		}
	}

	// Build the embedded post view
	embedView := &bsky.EmbedRecord_ViewRecord{
		LexiconTypeID: "app.bsky.embed.record#viewRecord",
		Uri:           quotedPost.URI,
		Cid:           quotedPost.Cid,
		Author:        authorView,
		Value: &util.LexiconTypeDecoder{
			Val: quotedPost.Post,
		},
		IndexedAt: quotedPost.Post.CreatedAt,
	}

	// Add engagement counts
	if quotedPost.LikeCount > 0 {
		lc := int64(quotedPost.LikeCount)
		embedView.LikeCount = &lc
	}
	if quotedPost.RepostCount > 0 {
		rc := int64(quotedPost.RepostCount)
		embedView.RepostCount = &rc
	}
	if quotedPost.ReplyCount > 0 {
		rpc := int64(quotedPost.ReplyCount)
		embedView.ReplyCount = &rpc
	}

	// Note: We don't recursively hydrate embeds for quoted posts to avoid deep nesting
	// The official app also doesn't show embeds within quoted posts

	return &bsky.EmbedRecord_View_Record{
		EmbedRecord_ViewRecord: embedView,
	}
}

// isPostURI checks if a URI is a post URI
func isPostURI(uri string) bool {
	return len(uri) > 5 && uri[:5] == "at://" && (
	// Check if it contains /app.bsky.feed.post/
	len(uri) > 25 && uri[len(uri)-25:len(uri)-12] == "/app.bsky.feed.post/" ||
		// More flexible check
		contains(uri, "/app.bsky.feed.post/"))
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

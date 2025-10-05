package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
	"github.com/whyrusleeping/market/models"
)

func (s *Server) runApiServer() error {

	e := echo.New()
	e.Use(middleware.CORS())
	e.GET("/debug", s.handleGetDebugInfo)

	views := e.Group("/api")
	views.GET("/me", s.handleGetMe)
	views.GET("/notifications", s.handleGetNotifications)
	views.GET("/profile/:account/post/:rkey", s.handleGetPost)
	views.GET("/profile/:account", s.handleGetProfileView)
	views.GET("/profile/:account/posts", s.handleGetProfilePosts)
	views.GET("/followingfeed", s.handleGetFollowingFeed)
	views.GET("/thread/:postid", s.handleGetThread)
	views.GET("/post/:postid/likes", s.handleGetPostLikes)
	views.GET("/post/:postid/reposts", s.handleGetPostReposts)
	views.GET("/post/:postid/replies", s.handleGetPostReplies)
	views.POST("/createRecord", s.handleCreateRecord)

	return e.Start(":4444")
}

func (s *Server) handleGetDebugInfo(e echo.Context) error {
	s.seqLk.Lock()
	seq := s.lastSeq
	s.seqLk.Unlock()

	return e.JSON(200, map[string]any{
		"seq": seq,
	})
}

func (s *Server) handleGetMe(e echo.Context) error {
	ctx := e.Request().Context()

	resp, err := s.dir.LookupDID(ctx, syntax.DID(s.mydid))
	if err != nil {
		return e.JSON(500, map[string]any{
			"error": "failed to lookup handle",
		})
	}

	return e.JSON(200, map[string]any{
		"did":    s.mydid,
		"handle": resp.Handle.String(),
	})
}

func (s *Server) handleGetPost(e echo.Context) error {
	ctx := e.Request().Context()

	account := e.Param("account")
	rkey := e.Param("rkey")

	did, err := s.resolveAccountIdent(ctx, account)
	if err != nil {
		return err
	}

	postUri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", did, rkey)

	p, err := s.backend.getPostByUri(ctx, postUri, "*")
	if err != nil {
		return err
	}

	if p.Raw == nil {
		return e.JSON(404, map[string]any{
			"error": "missing post",
		})
	}

	var fp bsky.FeedPost
	if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
		return nil
	}

	return e.JSON(200, fp)
}

func (s *Server) handleGetProfileView(e echo.Context) error {
	ctx := e.Request().Context()

	account := e.Param("account")

	accdid, err := s.resolveAccountIdent(ctx, account)
	if err != nil {
		return err
	}

	r, err := s.backend.getOrCreateRepo(ctx, accdid)
	if err != nil {
		return err
	}

	var profile models.Profile
	if err := s.backend.db.Find(&profile, "repo = ?", r.ID).Error; err != nil {
		return err
	}

	if profile.Raw == nil || len(profile.Raw) == 0 {
		s.addMissingProfile(ctx, accdid)
		return e.JSON(404, map[string]any{
			"error": "missing profile info for user",
		})
	}

	var prof bsky.ActorProfile
	if err := prof.UnmarshalCBOR(bytes.NewReader(profile.Raw)); err != nil {
		return err
	}

	return e.JSON(200, prof)
}

func (s *Server) handleGetProfilePosts(e echo.Context) error {
	ctx := e.Request().Context()

	account := e.Param("account")

	accdid, err := s.resolveAccountIdent(ctx, account)
	if err != nil {
		return err
	}

	r, err := s.backend.getOrCreateRepo(ctx, accdid)
	if err != nil {
		return err
	}

	// Get cursor from query parameter (timestamp in RFC3339 format)
	cursor := e.QueryParam("cursor")
	limit := 50

	tcursor := time.Now()
	if cursor != "" {
		t, err := time.Parse(time.RFC3339, cursor)
		if err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		tcursor = t
	}

	var dbposts []models.Post
	if err := s.backend.db.Raw("SELECT * FROM posts WHERE author = ? AND created < ? ORDER BY created DESC LIMIT ?", r.ID, tcursor, limit).Scan(&dbposts).Error; err != nil {
		return err
	}

	posts := s.hydratePosts(ctx, dbposts)

	// Generate next cursor from the last post's timestamp
	var nextCursor string
	if len(dbposts) > 0 {
		nextCursor = dbposts[len(dbposts)-1].Created.Format(time.RFC3339)
	}

	return e.JSON(200, map[string]any{
		"posts":  posts,
		"cursor": nextCursor,
	})
}

type postCounts struct {
	Likes   int `json:"likes"`
	Reposts int `json:"reposts"`
	Replies int `json:"replies"`
}

type embedRecordView struct {
	Type   string         `json:"$type"`
	Uri    string         `json:"uri"`
	Cid    string         `json:"cid"`
	Author *authorInfo    `json:"author,omitempty"`
	Value  *bsky.FeedPost `json:"value,omitempty"`
}

type viewerLike struct {
	Uri string `json:"uri"`
	Cid string `json:"cid"`
}

type postResponse struct {
	Missing    bool          `json:"missing"`
	Uri        string        `json:"uri"`
	Cid        string        `json:"cid"`
	Post       *feedPostView `json:"post"`
	AuthorInfo *authorInfo   `json:"author"`
	Counts     *postCounts   `json:"counts"`
	ViewerLike *viewerLike   `json:"viewerLike,omitempty"`

	ID         uint `json:"id"`
	ReplyTo    uint `json:"replyTo,omitempty"`
	ReplyToUsr uint `json:"replyToUsr,omitempty"`
	InThread   uint `json:"inThread,omitempty"`
}

type feedPostView struct {
	Type      string      `json:"$type"`
	CreatedAt string      `json:"createdAt"`
	Langs     []string    `json:"langs,omitempty"`
	Text      string      `json:"text"`
	Facets    interface{} `json:"facets,omitempty"`
	Embed     interface{} `json:"embed,omitempty"`
}

type authorInfo struct {
	Handle  string             `json:"handle"`
	Did     string             `json:"did"`
	Profile *bsky.ActorProfile `json:"profile"`
}

func (s *Server) handleGetFollowingFeed(e echo.Context) error {
	ctx := e.Request().Context()

	myr, err := s.backend.getOrCreateRepo(ctx, s.mydid)
	if err != nil {
		return err
	}

	// Get cursor from query parameter (timestamp in RFC3339 format)
	cursor := e.QueryParam("cursor")
	limit := 20

	tcursor := time.Now()
	if cursor != "" {
		t, err := time.Parse(time.RFC3339, cursor)
		if err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		tcursor = t
	}
	var dbposts []models.Post
	if err := s.backend.db.Raw("select * from posts where reply_to = 0 AND author IN (select subject from follows where author = ?) AND created < ? order by created DESC limit ?", myr.ID, tcursor, limit).Scan(&dbposts).Error; err != nil {
		return err
	}

	posts := s.hydratePosts(ctx, dbposts)

	// Generate next cursor from the last post's timestamp
	var nextCursor string
	if len(dbposts) > 0 {
		nextCursor = dbposts[len(dbposts)-1].Created.Format(time.RFC3339)
	}

	return e.JSON(200, map[string]any{
		"posts":  posts,
		"cursor": nextCursor,
	})
}

func (s *Server) getAuthorInfo(ctx context.Context, r *models.Repo) (*authorInfo, error) {
	var profile models.Profile
	if err := s.backend.db.Find(&profile, "repo = ?", r.ID).Error; err != nil {
		return nil, err
	}

	resp, err := s.dir.LookupDID(ctx, syntax.DID(r.Did))
	if err != nil {
		return nil, err
	}

	if profile.Raw == nil || len(profile.Raw) == 0 {
		s.addMissingProfile(ctx, r.Did)
		return &authorInfo{
			Handle: resp.Handle.String(),
			Did:    r.Did,
		}, nil
	}

	var prof bsky.ActorProfile
	if err := prof.UnmarshalCBOR(bytes.NewReader(profile.Raw)); err != nil {
		return nil, err
	}

	return &authorInfo{
		Handle:  resp.Handle.String(),
		Did:     r.Did,
		Profile: &prof,
	}, nil
}

func (s *Server) getPostCounts(ctx context.Context, pid uint) (*postCounts, error) {
	var pc postCounts
	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()
		if err := s.backend.db.Raw("SELECT count(*) FROM likes WHERE subject = ?", pid).Scan(&pc.Likes).Error; err != nil {
			slog.Error("failed to get likes count", "post", pid, "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := s.backend.db.Raw("SELECT count(*) FROM reposts WHERE subject = ?", pid).Scan(&pc.Reposts).Error; err != nil {
			slog.Error("failed to get reposts count", "post", pid, "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := s.backend.db.Raw("SELECT count(*) FROM posts WHERE reply_to = ?", pid).Scan(&pc.Replies).Error; err != nil {
			slog.Error("failed to get replies count", "post", pid, "error", err)
		}
	}()

	wg.Wait()

	return &pc, nil
}

func (s *Server) hydratePosts(ctx context.Context, dbposts []models.Post) []postResponse {
	posts := make([]postResponse, len(dbposts))
	var wg sync.WaitGroup

	for i := range dbposts {
		wg.Add(1)
		go func(ix int) {
			defer wg.Done()
			p := dbposts[ix]
			r, err := s.backend.getRepoByID(ctx, p.Author)
			if err != nil {
				fmt.Println("failed to get repo: ", err)
				posts[ix] = postResponse{
					Uri:     "",
					Missing: true,
				}
				return
			}

			uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", r.Did, p.Rkey)
			if len(p.Raw) == 0 || p.NotFound {
				s.addMissingPost(ctx, uri)
				posts[ix] = postResponse{
					Uri:     uri,
					Missing: true,
				}
				return
			}

			var fp bsky.FeedPost
			if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
				log.Warn("failed to unmarshal post", "uri", uri, "error", err)
				posts[ix] = postResponse{
					Uri:     uri,
					Missing: true,
				}
				return
			}

			author, err := s.getAuthorInfo(ctx, r)
			if err != nil {
				slog.Error("failed to load author info for post", "error", err)
			}

			counts, err := s.getPostCounts(ctx, p.ID)
			if err != nil {
				slog.Error("failed to get counts for post", "post", p.ID, "error", err)
			}

			// Build post view with hydrated embeds
			postView := s.buildPostView(ctx, &fp)

			viewerLike := s.checkViewerLike(ctx, p.ID)

			posts[ix] = postResponse{
				Uri:        uri,
				Cid:        p.Cid,
				Post:       postView,
				AuthorInfo: author,
				Counts:     counts,
				ID:         p.ID,
				ReplyTo:    p.ReplyTo,
				ReplyToUsr: p.ReplyToUsr,
				InThread:   p.InThread,

				ViewerLike: viewerLike,
			}
		}(i)
	}

	wg.Wait()

	return posts
}

func (s *Server) checkViewerLike(ctx context.Context, pid uint) *viewerLike {
	var like Like
	if err := s.backend.db.Raw("SELECT * FROM likes WHERE subject = ? AND author = ?", pid, s.myrepo.ID).Scan(&like).Error; err != nil {
		slog.Error("failed to lookup like", "error", err)
		return nil
	}

	if like.ID == 0 {
		return nil
	}

	uri := fmt.Sprintf("at://%s/app.bsky.feed.like/%s", s.myrepo.Did, like.Rkey)

	return &viewerLike{
		Uri: uri,
		Cid: like.Cid,
	}
}

func (s *Server) buildPostView(ctx context.Context, fp *bsky.FeedPost) *feedPostView {
	view := &feedPostView{
		Type:      fp.LexiconTypeID,
		CreatedAt: fp.CreatedAt,
		Text:      fp.Text,
		Facets:    fp.Facets,
	}

	if fp.Langs != nil {
		view.Langs = fp.Langs
	}

	// Hydrate embed if present
	if fp.Embed != nil {
		slog.Info("processing embed", "hasImages", fp.Embed.EmbedImages != nil, "hasExternal", fp.Embed.EmbedExternal != nil, "hasRecord", fp.Embed.EmbedRecord != nil)
		if fp.Embed.EmbedImages != nil {
			view.Embed = fp.Embed.EmbedImages
		} else if fp.Embed.EmbedExternal != nil {
			view.Embed = fp.Embed.EmbedExternal
		} else if fp.Embed.EmbedRecord != nil {
			// Hydrate quoted post
			quotedURI := fp.Embed.EmbedRecord.Record.Uri
			quotedCid := fp.Embed.EmbedRecord.Record.Cid
			slog.Info("hydrating quoted post", "uri", quotedURI, "cid", quotedCid)

			quotedPost, err := s.backend.getPostByUri(ctx, quotedURI, "*")
			if err != nil {
				slog.Warn("failed to get quoted post", "uri", quotedURI, "error", err)
			}
			if err == nil && quotedPost != nil && quotedPost.Raw != nil && len(quotedPost.Raw) > 0 && !quotedPost.NotFound {
				slog.Info("found quoted post, hydrating")
				var quotedFP bsky.FeedPost
				if err := quotedFP.UnmarshalCBOR(bytes.NewReader(quotedPost.Raw)); err == nil {
					quotedRepo, err := s.backend.getRepoByID(ctx, quotedPost.Author)
					if err == nil {
						quotedAuthor, err := s.getAuthorInfo(ctx, quotedRepo)
						if err == nil {
							view.Embed = map[string]interface{}{
								"$type": "app.bsky.embed.record",
								"record": &embedRecordView{
									Type:   "app.bsky.embed.record#viewRecord",
									Uri:    quotedURI,
									Cid:    quotedCid,
									Author: quotedAuthor,
									Value:  &quotedFP,
								},
							}
						}
					}
				}
			}

			// Fallback if hydration failed - show basic info
			if view.Embed == nil {
				slog.Info("quoted post not in database, using fallback")
				view.Embed = map[string]interface{}{
					"$type": "app.bsky.embed.record",
					"record": map[string]interface{}{
						"uri": quotedURI,
						"cid": quotedCid,
					},
				}
			}
		}
	}

	return view
}

func (s *Server) handleGetThread(e echo.Context) error {
	ctx := e.Request().Context()

	postIDStr := e.Param("postid")
	var postID uint
	if _, err := fmt.Sscanf(postIDStr, "%d", &postID); err != nil {
		return e.JSON(400, map[string]any{
			"error": "invalid post ID",
		})
	}

	// Get the requested post to find the thread root
	var requestedPost models.Post
	if err := s.backend.db.Find(&requestedPost, "id = ?", postID).Error; err != nil {
		return err
	}

	if requestedPost.ID == 0 {
		return e.JSON(404, map[string]any{
			"error": "post not found",
		})
	}

	// Determine the root post ID
	rootPostID := postID
	if requestedPost.InThread != 0 {
		rootPostID = requestedPost.InThread
	}

	// Get all posts in this thread
	var dbposts []models.Post
	query := "SELECT * FROM posts WHERE id = ? OR in_thread = ? ORDER BY created ASC"
	if err := s.backend.db.Raw(query, rootPostID, rootPostID).Scan(&dbposts).Error; err != nil {
		return err
	}

	// Build response for each post
	posts := []postResponse{}
	for _, p := range dbposts {
		r, err := s.backend.getRepoByID(ctx, p.Author)
		if err != nil {
			return err
		}

		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", r.Did, p.Rkey)
		if len(p.Raw) == 0 || p.NotFound {
			posts = append(posts, postResponse{
				Uri:        uri,
				Missing:    true,
				ReplyTo:    p.ReplyTo,
				ReplyToUsr: p.ReplyToUsr,
				InThread:   p.InThread,
			})
			continue
		}

		var fp bsky.FeedPost
		if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
			return err
		}

		author, err := s.getAuthorInfo(ctx, r)
		if err != nil {
			slog.Error("failed to load author info for post", "error", err)
		}

		counts, err := s.getPostCounts(ctx, p.ID)
		if err != nil {
			slog.Error("failed to get counts for post", "post", p.ID, "error", err)
		}

		// Build post view with hydrated embeds
		postView := s.buildPostView(ctx, &fp)

		posts = append(posts, postResponse{
			Uri:        uri,
			Cid:        p.Cid,
			Post:       postView,
			AuthorInfo: author,
			Counts:     counts,
			ID:         p.ID,
			ReplyTo:    p.ReplyTo,
			ReplyToUsr: p.ReplyToUsr,
			InThread:   p.InThread,
		})
	}

	return e.JSON(200, map[string]any{
		"posts":      posts,
		"rootPostId": rootPostID,
	})
}

type engagementUser struct {
	Handle  string             `json:"handle"`
	Did     string             `json:"did"`
	Profile *bsky.ActorProfile `json:"profile,omitempty"`
	Time    string             `json:"time"`
}

func (s *Server) handleGetPostLikes(e echo.Context) error {
	ctx := e.Request().Context()

	postIDStr := e.Param("postid")
	var postID uint
	if _, err := fmt.Sscanf(postIDStr, "%d", &postID); err != nil {
		return e.JSON(400, map[string]any{
			"error": "invalid post ID",
		})
	}

	// Get all likes for this post
	var likes []models.Like
	if err := s.backend.db.Find(&likes, "subject = ?", postID).Error; err != nil {
		return err
	}

	users := []engagementUser{}
	for _, like := range likes {
		r, err := s.backend.getRepoByID(ctx, like.Author)
		if err != nil {
			slog.Error("failed to get repo for like author", "error", err)
			continue
		}

		// Look up handle
		resp, err := s.dir.LookupDID(ctx, syntax.DID(r.Did))
		if err != nil {
			slog.Error("failed to lookup DID", "did", r.Did, "error", err)
			continue
		}

		// Get profile if available
		var profile models.Profile
		s.backend.db.Find(&profile, "repo = ?", r.ID)

		var prof *bsky.ActorProfile
		if len(profile.Raw) > 0 {
			var p bsky.ActorProfile
			if err := p.UnmarshalCBOR(bytes.NewReader(profile.Raw)); err == nil {
				prof = &p
			}
		} else {
			s.addMissingProfile(ctx, r.Did)
		}

		users = append(users, engagementUser{
			Handle:  resp.Handle.String(),
			Did:     r.Did,
			Profile: prof,
			Time:    like.Created.Format("2006-01-02T15:04:05Z"),
		})
	}

	return e.JSON(200, map[string]any{
		"users": users,
		"count": len(users),
	})
}

func (s *Server) handleGetPostReposts(e echo.Context) error {
	ctx := e.Request().Context()

	postIDStr := e.Param("postid")
	var postID uint
	if _, err := fmt.Sscanf(postIDStr, "%d", &postID); err != nil {
		return e.JSON(400, map[string]any{
			"error": "invalid post ID",
		})
	}

	// Get all reposts for this post
	var reposts []models.Repost
	if err := s.backend.db.Find(&reposts, "subject = ?", postID).Error; err != nil {
		return err
	}

	users := []engagementUser{}
	for _, repost := range reposts {
		r, err := s.backend.getRepoByID(ctx, repost.Author)
		if err != nil {
			slog.Error("failed to get repo for repost author", "error", err)
			continue
		}

		// Look up handle
		resp, err := s.dir.LookupDID(ctx, syntax.DID(r.Did))
		if err != nil {
			slog.Error("failed to lookup DID", "did", r.Did, "error", err)
			continue
		}

		// Get profile if available
		var profile models.Profile
		s.backend.db.Find(&profile, "repo = ?", r.ID)

		var prof *bsky.ActorProfile
		if len(profile.Raw) > 0 {
			var p bsky.ActorProfile
			if err := p.UnmarshalCBOR(bytes.NewReader(profile.Raw)); err == nil {
				prof = &p
			}
		} else {
			s.addMissingProfile(ctx, r.Did)
		}

		users = append(users, engagementUser{
			Handle:  resp.Handle.String(),
			Did:     r.Did,
			Profile: prof,
			Time:    repost.Created.Format("2006-01-02T15:04:05Z"),
		})
	}

	return e.JSON(200, map[string]any{
		"users": users,
		"count": len(users),
	})
}

func (s *Server) handleGetPostReplies(e echo.Context) error {
	ctx := e.Request().Context()

	postIDStr := e.Param("postid")
	var postID uint
	if _, err := fmt.Sscanf(postIDStr, "%d", &postID); err != nil {
		return e.JSON(400, map[string]any{
			"error": "invalid post ID",
		})
	}

	// Get all replies to this post
	var replies []models.Post
	if err := s.backend.db.Find(&replies, "reply_to = ?", postID).Error; err != nil {
		return err
	}

	users := []engagementUser{}
	seen := make(map[uint]bool) // Track unique authors

	for _, reply := range replies {
		// Skip if we've already added this author
		if seen[reply.Author] {
			continue
		}
		seen[reply.Author] = true

		r, err := s.backend.getRepoByID(ctx, reply.Author)
		if err != nil {
			slog.Error("failed to get repo for reply author", "error", err)
			continue
		}

		// Look up handle
		resp, err := s.dir.LookupDID(ctx, syntax.DID(r.Did))
		if err != nil {
			slog.Error("failed to lookup DID", "did", r.Did, "error", err)
			continue
		}

		// Get profile if available
		var profile models.Profile
		s.backend.db.Find(&profile, "repo = ?", r.ID)

		var prof *bsky.ActorProfile
		if len(profile.Raw) > 0 {
			var p bsky.ActorProfile
			if err := p.UnmarshalCBOR(bytes.NewReader(profile.Raw)); err == nil {
				prof = &p
			}
		} else {
			s.addMissingProfile(ctx, r.Did)
		}

		users = append(users, engagementUser{
			Handle:  resp.Handle.String(),
			Did:     r.Did,
			Profile: prof,
			Time:    reply.Created.Format("2006-01-02T15:04:05Z"),
		})
	}

	return e.JSON(200, map[string]any{
		"users": users,
		"count": len(users),
	})
}

type createRecordRequest struct {
	Collection string         `json:"collection"`
	Record     map[string]any `json:"record"`
}

type createRecordResponse struct {
	Uri string `json:"uri"`
	Cid string `json:"cid"`
}

func (s *Server) handleCreateRecord(e echo.Context) error {
	ctx := e.Request().Context()

	var req createRecordRequest
	if err := e.Bind(&req); err != nil {
		return e.JSON(400, map[string]any{
			"error": "invalid request",
		})
	}

	// Marshal the record to JSON for XRPC
	recordBytes, err := json.Marshal(req.Record)
	if err != nil {
		slog.Error("failed to marshal record", "error", err)
		return e.JSON(400, map[string]any{
			"error": "invalid record",
		})
	}

	// Create the input for the repo.createRecord call
	input := map[string]any{
		"repo":       s.mydid,
		"collection": req.Collection,
		"record":     json.RawMessage(recordBytes),
	}

	var resp createRecordResponse
	if err := s.client.Do(ctx, xrpc.Procedure, "application/json", "com.atproto.repo.createRecord", nil, input, &resp); err != nil {
		slog.Error("failed to create record", "error", err)
		return e.JSON(500, map[string]any{
			"error":   "failed to create record",
			"details": err.Error(),
		})
	}

	return e.JSON(200, resp)
}

type notificationResponse struct {
	ID         uint        `json:"id"`
	Kind       string      `json:"kind"`
	Author     *authorInfo `json:"author"`
	Source     string      `json:"source"`
	SourcePost *struct {
		Text string `json:"text"`
		Uri  string `json:"uri"`
	} `json:"sourcePost,omitempty"`
	CreatedAt string `json:"createdAt"`
}

func (s *Server) handleGetNotifications(e echo.Context) error {
	ctx := e.Request().Context()

	// Get cursor from query parameter (notification ID)
	cursor := e.QueryParam("cursor")
	limit := 50

	var cursorID uint
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &cursorID); err != nil {
			return e.JSON(400, map[string]any{
				"error": "invalid cursor",
			})
		}
	}

	// Query notifications
	var notifications []Notification
	query := `SELECT * FROM notifications WHERE "for" = ?`
	if cursorID > 0 {
		query += ` AND id < ?`
		if err := s.backend.db.Raw(query+" ORDER BY created_at DESC LIMIT ?", s.myrepo.ID, cursorID, limit).Scan(&notifications).Error; err != nil {
			return err
		}
	} else {
		if err := s.backend.db.Raw(query+" ORDER BY created_at DESC LIMIT ?", s.myrepo.ID, limit).Scan(&notifications).Error; err != nil {
			return err
		}
	}

	// Hydrate notifications
	results := []notificationResponse{}
	for _, notif := range notifications {
		// Get author info
		author, err := s.backend.getRepoByID(ctx, notif.Author)
		if err != nil {
			slog.Error("failed to get repo for notification author", "error", err)
			continue
		}

		authorInfo, err := s.getAuthorInfo(ctx, author)
		if err != nil {
			slog.Error("failed to get author info", "error", err)
			continue
		}

		resp := notificationResponse{
			ID:        notif.ID,
			Kind:      notif.Kind,
			Author:    authorInfo,
			Source:    notif.Source,
			CreatedAt: notif.CreatedAt.Format(time.RFC3339),
		}

		// Try to get source post preview for reply/mention notifications
		if notif.Kind == NotifKindReply || notif.Kind == NotifKindMention {
			// Parse URI to get post
			p, err := s.backend.getPostByUri(ctx, notif.Source, "*")
			if err == nil && p.Raw != nil && len(p.Raw) > 0 {
				var fp bsky.FeedPost
				if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err == nil {
					preview := fp.Text
					if len(preview) > 100 {
						preview = preview[:100] + "..."
					}
					resp.SourcePost = &struct {
						Text string `json:"text"`
						Uri  string `json:"uri"`
					}{
						Text: preview,
						Uri:  notif.Source,
					}
				}
			}
		}

		results = append(results, resp)
	}

	// Generate next cursor
	var nextCursor string
	if len(notifications) > 0 {
		nextCursor = fmt.Sprintf("%d", notifications[len(notifications)-1].ID)
	}

	return e.JSON(200, map[string]any{
		"notifications": results,
		"cursor":        nextCursor,
	})
}

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
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
	views.GET("/profile/:account/post/:rkey", s.handleGetPost)
	views.GET("/profile/:account", s.handleGetProfileView)
	views.GET("/profile/:account/posts", s.handleGetProfilePosts)
	views.GET("/followingfeed", s.handleGetFollowingFeed)
	views.GET("/thread/:postid", s.handleGetThread)
	views.GET("/post/:postid/likes", s.handleGetPostLikes)
	views.GET("/post/:postid/reposts", s.handleGetPostReposts)
	views.GET("/post/:postid/replies", s.handleGetPostReplies)

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

	var dbposts []models.Post
	if err := s.backend.db.Find(&dbposts, "author = ?", r.ID).Error; err != nil {
		return err
	}

	author, err := s.getAuthorInfo(ctx, r)
	if err != nil {
		slog.Error("failed to load author info for post", "error", err)
	}

	posts := []postResponse{}
	for _, p := range dbposts {
		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", r.Did, p.Rkey)
		if len(p.Raw) == 0 || p.NotFound {
			posts = append(posts, postResponse{
				Uri:     uri,
				Missing: true,
			})
			continue
		}
		var fp bsky.FeedPost
		if err := fp.UnmarshalCBOR(bytes.NewReader(p.Raw)); err != nil {
			return err
		}

		counts, err := s.getPostCounts(ctx, p.ID)
		if err != nil {
			slog.Error("failed to get counts for post", "post", p.ID, "error", err)
		}

		posts = append(posts, postResponse{
			Uri:        uri,
			Post:       &fp,
			AuthorInfo: author,
			Counts:     counts,
			ID:         p.ID,
			ReplyTo:    p.ReplyTo,
			ReplyToUsr: p.ReplyToUsr,
			InThread:   p.InThread,
		})
	}

	return e.JSON(200, posts)
}

type postCounts struct {
	Likes   int `json:"likes"`
	Reposts int `json:"reposts"`
	Replies int `json:"replies"`
}

type postResponse struct {
	Missing    bool           `json:"missing"`
	Uri        string         `json:"uri"`
	Post       *bsky.FeedPost `json:"post"`
	AuthorInfo *authorInfo    `json:"author"`
	Counts     *postCounts    `json:"counts"`
	ID         uint           `json:"id"`
	ReplyTo    uint           `json:"replyTo,omitempty"`
	ReplyToUsr uint           `json:"replyToUsr,omitempty"`
	InThread   uint           `json:"inThread,omitempty"`
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

			posts[ix] = postResponse{
				Uri:        uri,
				Post:       &fp,
				AuthorInfo: author,
				Counts:     counts,
				ID:         p.ID,
				ReplyTo:    p.ReplyTo,
				ReplyToUsr: p.ReplyToUsr,
				InThread:   p.InThread,
			}
		}(i)
	}

	wg.Wait()

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
	if err := s.backend.db.Raw("SELECT count(*) FROM likes WHERE subject = ?", pid).Scan(&pc.Likes).Error; err != nil {
		return nil, err
	}
	if err := s.backend.db.Raw("SELECT count(*) FROM reposts WHERE subject = ?", pid).Scan(&pc.Reposts).Error; err != nil {
		return nil, err
	}
	if err := s.backend.db.Raw("SELECT count(*) FROM posts WHERE reply_to = ?", pid).Scan(&pc.Replies).Error; err != nil {
		return nil, err
	}

	return &pc, nil
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

		posts = append(posts, postResponse{
			Uri:        uri,
			Post:       &fp,
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

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
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

	var dbposts []models.Post
	if err := s.backend.db.Raw("select * from posts where reply_to = 0 AND author IN (select subject from follows where author = ?) order by created DESC limit 10 ", myr.ID).Scan(&dbposts).Error; err != nil {
		return err
	}

	posts := []postResponse{}
	for _, p := range dbposts {
		r, err := s.backend.getRepoByID(ctx, p.Author)
		if err != nil {
			return err
		}

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
		})
	}

	return e.JSON(200, posts)
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

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	xrpclib "github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
)

func (s *Server) addMissingProfile(ctx context.Context, did string) {
	select {
	case s.missingProfiles <- did:
	case <-ctx.Done():
	}
}

func (s *Server) missingProfileFetcher() {
	for did := range s.missingProfiles {
		if err := s.fetchMissingProfile(context.TODO(), did); err != nil {
			log.Warn("failed to fetch missing profile", "did", did, "error", err)
		}
	}
}

func (s *Server) fetchMissingProfile(ctx context.Context, did string) error {
	repo, err := s.backend.getOrCreateRepo(ctx, did)
	if err != nil {
		return err
	}

	resp, err := s.dir.LookupDID(ctx, syntax.DID(did))
	if err != nil {
		return err
	}

	c := &xrpclib.Client{
		Host: resp.PDSEndpoint(),
	}

	rec, err := atproto.RepoGetRecord(ctx, c, "", "app.bsky.actor.profile", did, "self")
	if err != nil {
		return err
	}

	prof, ok := rec.Value.Val.(*bsky.ActorProfile)
	if !ok {
		return fmt.Errorf("record we got back wasnt a profile somehow")
	}

	buf := new(bytes.Buffer)
	if err := prof.MarshalCBOR(buf); err != nil {
		return err
	}

	cc, err := cid.Decode(*rec.Cid)
	if err != nil {
		return err
	}

	return s.backend.HandleUpdateProfile(ctx, repo, "self", "", buf.Bytes(), cc)
}

func (s *Server) addMissingPost(ctx context.Context, uri string) {
	slog.Info("adding missing post to fetch queue", "uri", uri)
	select {
	case s.missingPosts <- uri:
	case <-ctx.Done():
	}
}

func (s *Server) missingPostFetcher() {
	for uri := range s.missingPosts {
		if err := s.fetchMissingPost(context.TODO(), uri); err != nil {
			log.Warn("failed to fetch missing post", "uri", uri, "error", err)
		}
	}
}

func (s *Server) fetchMissingPost(ctx context.Context, uri string) error {
	// Parse AT URI: at://did:plc:xxx/app.bsky.feed.post/rkey
	parts := strings.Split(uri, "/")
	if len(parts) < 5 || !strings.HasPrefix(parts[2], "did:") {
		return fmt.Errorf("invalid AT URI: %s", uri)
	}

	did := parts[2]
	collection := parts[3]
	rkey := parts[4]

	repo, err := s.backend.getOrCreateRepo(ctx, did)
	if err != nil {
		return err
	}

	resp, err := s.dir.LookupDID(ctx, syntax.DID(did))
	if err != nil {
		return err
	}

	c := &xrpclib.Client{
		Host: resp.PDSEndpoint(),
	}

	rec, err := atproto.RepoGetRecord(ctx, c, "", collection, did, rkey)
	if err != nil {
		return err
	}

	post, ok := rec.Value.Val.(*bsky.FeedPost)
	if !ok {
		return fmt.Errorf("record we got back wasn't a post somehow")
	}

	buf := new(bytes.Buffer)
	if err := post.MarshalCBOR(buf); err != nil {
		return err
	}

	cc, err := cid.Decode(*rec.Cid)
	if err != nil {
		return err
	}

	return s.backend.HandleCreatePost(ctx, repo, rkey, buf.Bytes(), cc)
}

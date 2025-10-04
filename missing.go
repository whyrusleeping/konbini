package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
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

	c := &xrpc.Client{
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

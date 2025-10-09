package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	xrpclib "github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
)

type MissingRecordType string

const (
	MissingRecordTypeProfile       MissingRecordType = "profile"
	MissingRecordTypePost          MissingRecordType = "post"
	MissingRecordTypeFeedGenerator MissingRecordType = "feedgenerator"
)

type MissingRecord struct {
	Type       MissingRecordType
	Identifier string // DID for profiles, AT-URI for posts/feedgens
}

func (s *Server) addMissingRecord(ctx context.Context, rec MissingRecord) {
	select {
	case s.missingRecords <- rec:
	case <-ctx.Done():
	}
}

// Legacy methods for backward compatibility
func (s *Server) addMissingProfile(ctx context.Context, did string) {
	s.addMissingRecord(ctx, MissingRecord{
		Type:       MissingRecordTypeProfile,
		Identifier: did,
	})
}

func (s *Server) addMissingPost(ctx context.Context, uri string) {
	slog.Info("adding missing post to fetch queue", "uri", uri)
	s.addMissingRecord(ctx, MissingRecord{
		Type:       MissingRecordTypePost,
		Identifier: uri,
	})
}

func (s *Server) addMissingFeedGenerator(ctx context.Context, uri string) {
	slog.Info("adding missing feed generator to fetch queue", "uri", uri)
	s.addMissingRecord(ctx, MissingRecord{
		Type:       MissingRecordTypeFeedGenerator,
		Identifier: uri,
	})
}

func (s *Server) missingRecordFetcher() {
	for rec := range s.missingRecords {
		var err error
		switch rec.Type {
		case MissingRecordTypeProfile:
			err = s.fetchMissingProfile(context.TODO(), rec.Identifier)
		case MissingRecordTypePost:
			err = s.fetchMissingPost(context.TODO(), rec.Identifier)
		case MissingRecordTypeFeedGenerator:
			err = s.fetchMissingFeedGenerator(context.TODO(), rec.Identifier)
		default:
			slog.Error("unknown missing record type", "type", rec.Type)
			continue
		}

		if err != nil {
			slog.Warn("failed to fetch missing record", "type", rec.Type, "identifier", rec.Identifier, "error", err)
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

func (s *Server) fetchMissingPost(ctx context.Context, uri string) error {
	puri, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("invalid AT URI: %s", uri)
	}

	did := puri.Authority().String()
	collection := puri.Collection().String()
	rkey := puri.RecordKey().String()

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

func (s *Server) fetchMissingFeedGenerator(ctx context.Context, uri string) error {
	puri, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("invalid AT URI: %s", uri)
	}

	did := puri.Authority().String()
	collection := puri.Collection().String()
	rkey := puri.RecordKey().String()

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

	feedGen, ok := rec.Value.Val.(*bsky.FeedGenerator)
	if !ok {
		return fmt.Errorf("record we got back wasn't a feed generator somehow")
	}

	buf := new(bytes.Buffer)
	if err := feedGen.MarshalCBOR(buf); err != nil {
		return err
	}

	cc, err := cid.Decode(*rec.Cid)
	if err != nil {
		return err
	}

	return s.backend.HandleCreateFeedGenerator(ctx, repo, rkey, buf.Bytes(), cc)
}

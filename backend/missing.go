package backend

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
	MissingRecordTypeUnknown       MissingRecordType = "unknown"
)

type MissingRecord struct {
	Type       MissingRecordType
	Identifier string // DID for profiles, AT-URI for posts/feedgens
	Wait       bool

	waitch chan struct{}
}

func (b *PostgresBackend) addMissingRecord(ctx context.Context, rec MissingRecord) {
	if rec.Wait {
		rec.waitch = make(chan struct{})
	}

	select {
	case b.missingRecords <- rec:
	case <-ctx.Done():
	}

	if rec.Wait {
		select {
		case <-rec.waitch:
		case <-ctx.Done():
		}
	}
}

func (b *PostgresBackend) missingRecordFetcher() {
	for rec := range b.missingRecords {
		var err error
		switch rec.Type {
		case MissingRecordTypeProfile:
			err = b.fetchMissingProfile(context.TODO(), rec.Identifier)
		case MissingRecordTypePost:
			err = b.fetchMissingPost(context.TODO(), rec.Identifier)
		case MissingRecordTypeFeedGenerator:
			err = b.fetchMissingFeedGenerator(context.TODO(), rec.Identifier)
		default:
			slog.Error("unknown missing record type", "type", rec.Type)
			continue
		}

		if err != nil {
			slog.Warn("failed to fetch missing record", "type", rec.Type, "identifier", rec.Identifier, "error", err)
		}

		if rec.Wait {
			close(rec.waitch)
		}
	}
}

func (b *PostgresBackend) fetchMissingProfile(ctx context.Context, did string) error {
	b.AddRelevantDid(did)

	repo, err := b.GetOrCreateRepo(ctx, did)
	if err != nil {
		return err
	}

	resp, err := b.dir.LookupDID(ctx, syntax.DID(did))
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

	return b.HandleUpdateProfile(ctx, repo, "self", "", buf.Bytes(), cc)
}

func (b *PostgresBackend) fetchMissingPost(ctx context.Context, uri string) error {
	puri, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("invalid AT URI: %s", uri)
	}

	did := puri.Authority().String()
	collection := puri.Collection().String()
	rkey := puri.RecordKey().String()

	b.AddRelevantDid(did)

	repo, err := b.GetOrCreateRepo(ctx, did)
	if err != nil {
		return err
	}

	resp, err := b.dir.LookupDID(ctx, syntax.DID(did))
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

	return b.HandleCreatePost(ctx, repo, rkey, buf.Bytes(), cc)
}

func (b *PostgresBackend) fetchMissingFeedGenerator(ctx context.Context, uri string) error {
	puri, err := syntax.ParseATURI(uri)
	if err != nil {
		return fmt.Errorf("invalid AT URI: %s", uri)
	}

	did := puri.Authority().String()
	collection := puri.Collection().String()
	rkey := puri.RecordKey().String()
	b.AddRelevantDid(did)

	repo, err := b.GetOrCreateRepo(ctx, did)
	if err != nil {
		return err
	}

	resp, err := b.dir.LookupDID(ctx, syntax.DID(did))
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

	return b.HandleCreateFeedGenerator(ctx, repo, rkey, buf.Bytes(), cc)
}

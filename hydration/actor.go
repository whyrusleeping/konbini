package hydration

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

// ActorInfo contains hydrated actor information
type ActorInfo struct {
	DID     string
	Handle  string
	Profile *bsky.ActorProfile
}

// HydrateActor hydrates full actor information
func (h *Hydrator) HydrateActor(ctx context.Context, did string) (*ActorInfo, error) {
	// Look up handle
	resp, err := h.dir.LookupDID(ctx, syntax.DID(did))
	if err != nil {
		return nil, fmt.Errorf("failed to lookup DID: %w", err)
	}

	info := &ActorInfo{
		DID:    did,
		Handle: resp.Handle.String(),
	}

	// Load profile from database
	var dbProfile struct {
		Repo uint
		Raw  []byte
	}
	err = h.db.Raw("SELECT repo, raw FROM profiles WHERE repo = (SELECT id FROM repos WHERE did = ?)", did).
		Scan(&dbProfile).Error
	if err != nil {
		slog.Error("failed to fetch user profile", "error", err)
	} else {
		if len(dbProfile.Raw) > 0 {
			var profile bsky.ActorProfile
			if err := profile.UnmarshalCBOR(bytes.NewReader(dbProfile.Raw)); err == nil {
				info.Profile = &profile
			}
		} else {
			h.addMissingActor(did)
		}
	}

	return info, nil
}

type ActorInfoDetailed struct {
	ActorInfo
	FollowCount   int64
	FollowerCount int64
	PostCount     int64
}

func (h *Hydrator) HydrateActorDetailed(ctx context.Context, did string) (*ActorInfoDetailed, error) {
	act, err := h.HydrateActor(ctx, did)
	if err != nil {
		return nil, err
	}

	actd := ActorInfoDetailed{
		ActorInfo: *act,
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		c, err := h.getFollowCountForUser(ctx, did)
		if err != nil {
			slog.Error("failed to get follow count", "did", did, "error", err)
		}
		actd.FollowCount = c
	}()
	go func() {
		defer wg.Done()
		c, err := h.getFollowerCountForUser(ctx, did)
		if err != nil {
			slog.Error("failed to get follower count", "did", did, "error", err)
		}
		actd.FollowerCount = c
	}()
	go func() {
		defer wg.Done()
		c, err := h.getPostCountForUser(ctx, did)
		if err != nil {
			slog.Error("failed to get post count", "did", did, "error", err)
		}
		actd.PostCount = c
	}()
	wg.Wait()

	return &actd, nil
}

func (h *Hydrator) getFollowCountForUser(ctx context.Context, did string) (int64, error) {
	var count int64
	if err := h.db.Raw("SELECT count(*) FROM follows WHERE author = (SELECT id FROM repos WHERE did = ?)", did).Scan(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (h *Hydrator) getFollowerCountForUser(ctx context.Context, did string) (int64, error) {
	var count int64
	if err := h.db.Raw("SELECT count(*) FROM follows WHERE subject = (SELECT id FROM repos WHERE did = ?)", did).Scan(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (h *Hydrator) getPostCountForUser(ctx context.Context, did string) (int64, error) {
	var count int64
	if err := h.db.Raw("SELECT count(*) FROM posts WHERE author = (SELECT id FROM repos WHERE did = ?)", did).Scan(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

// HydrateActors hydrates multiple actors
func (h *Hydrator) HydrateActors(ctx context.Context, dids []string) (map[string]*ActorInfo, error) {
	result := make(map[string]*ActorInfo, len(dids))
	for _, did := range dids {
		info, err := h.HydrateActor(ctx, did)
		if err != nil {
			// Skip actors that fail to hydrate rather than failing the whole batch
			continue
		}
		result[did] = info
	}
	return result, nil
}

// ResolveDID resolves a handle or DID to a DID
func (h *Hydrator) ResolveDID(ctx context.Context, actor string) (string, error) {
	// If it's already a DID, return it
	if strings.HasPrefix(actor, "did:") {
		return actor, nil
	}

	// Otherwise, resolve the handle
	resp, err := h.dir.LookupHandle(ctx, syntax.Handle(actor))
	if err != nil {
		return "", fmt.Errorf("failed to resolve handle: %w", err)
	}

	return resp.DID.String(), nil
}

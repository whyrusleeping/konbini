package feed

import (
	"bytes"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	cid "github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	mh "github.com/multiformats/go-multihash"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetFeedGenerator implements app.bsky.feed.getFeedGenerator
func HandleGetFeedGenerator(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator, dir identity.Directory) error {
	ctx := c.Request().Context()

	// Parse parameters
	feedURI := c.QueryParam("feed")
	if feedURI == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":   "InvalidRequest",
			"message": "feed parameter is required",
		})
	}

	nu, err := hydrator.NormalizeUri(ctx, feedURI)
	if err != nil {
		return err
	}
	feedURI = nu

	viewer := getUserDID(c)
	_ = viewer

	// Extract feed generator DID and rkey from URI
	did := extractDIDFromURI(feedURI)
	rkey := extractRkeyFromURI(feedURI)

	if did == "" || rkey == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":   "InvalidRequest",
			"message": "invalid feed URI format",
		})
	}

	// Query feed generator from database
	type feedGenRow struct {
		ID        uint
		Did       string
		Raw       []byte
		AuthorDid string
		Indexed   time.Time
	}
	var feedGen feedGenRow
	err = db.Raw(`
		SELECT fg.id, fg.did, fg.raw, r.did as author_did, indexed
		FROM feed_generators fg
		JOIN repos r ON r.id = fg.author
		WHERE r.did = ? AND fg.rkey = ?
	`, did, rkey).Scan(&feedGen).Error

	if err != nil || feedGen.ID == 0 {
		// Track this missing feed generator for fetching
		hydrator.AddMissingRecord(feedURI, true)

		return c.JSON(http.StatusNotFound, map[string]any{
			"error":   "NotFound",
			"message": "feed generator not found",
		})
	}

	// Decode the feed generator record
	var feedGenRecord bsky.FeedGenerator
	if err := feedGenRecord.UnmarshalCBOR(bytes.NewReader(feedGen.Raw)); err != nil {
		slog.Error("failed to decode feed generator record", "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to decode feed generator record",
		})
	}

	// Compute CID from raw bytes
	hash, err := mh.Sum(feedGen.Raw, mh.SHA2_256, -1)
	if err != nil {
		slog.Error("failed to hash record", "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to compute CID",
		})
	}
	recordCid := cid.NewCidV1(cid.DagCBOR, hash).String()

	// Hydrate the creator
	creatorInfo, err := hydrator.HydrateActor(ctx, feedGen.AuthorDid)
	if err != nil {
		slog.Error("failed to hydrate creator", "error", err, "did", feedGen.AuthorDid)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to hydrate creator",
		})
	}

	// Count likes for this feed generator
	var likeCount int64

	// Check if viewer has liked this feed generator
	viewerLike := ""

	// Validate the service DID (check if it's resolvable)
	serviceDID, err := syntax.ParseDID(feedGenRecord.Did)
	if err != nil {
		slog.Error("invalid service DID in feed generator", "error", err, "did", feedGenRecord.Did)
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "invalid service DID",
		})
	}

	// Try to resolve the service DID to check if it's online/valid
	isOnline := true
	isValid := true
	serviceIdent, err := dir.LookupDID(ctx, serviceDID)
	if err != nil {
		slog.Warn("failed to resolve service DID", "error", err, "did", serviceDID)
		isOnline = false
		isValid = false
	} else {
		// Check if service has an endpoint
		serviceEndpoint := serviceIdent.PDSEndpoint()
		if serviceEndpoint == "" {
			slog.Warn("service has no PDS endpoint", "did", serviceDID)
			isValid = false
		}
	}

	// Build the generator view
	generatorView := views.GeneratorView(
		feedURI,
		recordCid,
		&feedGenRecord,
		creatorInfo,
		likeCount,
		viewerLike,
		feedGen.Indexed.Format(time.RFC3339),
	)

	output := &bsky.FeedGetFeedGenerator_Output{
		View:     generatorView,
		IsOnline: isOnline,
		IsValid:  isValid,
	}

	return c.JSON(http.StatusOK, output)
}

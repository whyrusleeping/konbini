package repo

import (
	"fmt"
	"net/http"

	cbg "github.com/whyrusleeping/cbor-gen"

	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"gorm.io/gorm"
)

// HandleGetRecord implements com.atproto.repo.getRecord
func HandleGetRecord(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	repoParam := c.QueryParam("repo")
	collection := c.QueryParam("collection")
	rkey := c.QueryParam("rkey")
	cidParam := c.QueryParam("cid")

	if repoParam == "" || collection == "" || rkey == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "repo, collection, and rkey parameters are required",
		})
	}

	ctx := c.Request().Context()

	// Resolve repo to DID
	repoDID, err := hydrator.ResolveDID(ctx, repoParam)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": fmt.Sprintf("could not find repo: %s", repoParam),
		})
	}

	// Build URI
	uri := fmt.Sprintf("at://%s/%s/%s", repoDID, collection, rkey)

	// Query the record based on collection type
	var recordCID string
	var recordRaw []byte

	switch collection {
	case "app.bsky.feed.post":
		type postRecord struct {
			CID string
			Raw []byte
		}
		var post postRecord
		err = db.Raw(`
			SELECT COALESCE(p.cid, '') as cid, p.raw
			FROM posts p
			JOIN repos r ON r.id = p.author
			WHERE r.did = ? AND p.rkey = ?
			LIMIT 1
		`, repoDID, rkey).Scan(&post).Error
		if err != nil || len(post.Raw) == 0 {
			return c.JSON(http.StatusNotFound, map[string]interface{}{
				"error":   "RecordNotFound",
				"message": fmt.Sprintf("could not locate record: %s", uri),
			})
		}
		recordCID = post.CID // May be empty
		recordRaw = post.Raw

	case "app.bsky.actor.profile":
		type profileRecord struct {
			CID string
			Raw []byte
		}
		var profile profileRecord
		err = db.Raw(`
			SELECT p.cid, p.raw
			FROM profiles p
			JOIN repos r ON r.id = p.repo
			WHERE r.did = ? AND p.rkey = ?
		`, repoDID, rkey).Scan(&profile).Error
		if err != nil || profile.CID == "" {
			return c.JSON(http.StatusNotFound, map[string]interface{}{
				"error":   "RecordNotFound",
				"message": fmt.Sprintf("could not locate record: %s", uri),
			})
		}
		recordCID = profile.CID
		recordRaw = profile.Raw

	case "app.bsky.graph.follow":
		type followRecord struct {
			CID string
			Raw []byte
		}
		var follow followRecord
		err = db.Raw(`
			SELECT f.cid, f.raw
			FROM follows f
			JOIN repos r ON r.id = f.author
			WHERE r.did = ? AND f.rkey = ?
		`, repoDID, rkey).Scan(&follow).Error
		if err != nil || follow.CID == "" {
			return c.JSON(http.StatusNotFound, map[string]interface{}{
				"error":   "RecordNotFound",
				"message": fmt.Sprintf("could not locate record: %s", uri),
			})
		}
		recordCID = follow.CID
		recordRaw = follow.Raw

	case "app.bsky.feed.like":
		type likeRecord struct {
			CID string
			Raw []byte
		}
		var like likeRecord
		err = db.Raw(`
			SELECT l.cid, l.raw
			FROM likes l
			JOIN repos r ON r.id = l.author
			WHERE r.did = ? AND l.rkey = ?
		`, repoDID, rkey).Scan(&like).Error
		if err != nil || like.CID == "" {
			return c.JSON(http.StatusNotFound, map[string]interface{}{
				"error":   "RecordNotFound",
				"message": fmt.Sprintf("could not locate record: %s", uri),
			})
		}
		recordCID = like.CID
		recordRaw = like.Raw

	case "app.bsky.feed.repost":
		type repostRecord struct {
			CID string
			Raw []byte
		}
		var repost repostRecord
		err = db.Raw(`
			SELECT rp.cid, rp.raw
			FROM reposts rp
			JOIN repos r ON r.id = rp.author
			WHERE r.did = ? AND rp.rkey = ?
		`, repoDID, rkey).Scan(&repost).Error
		if err != nil || repost.CID == "" {
			return c.JSON(http.StatusNotFound, map[string]interface{}{
				"error":   "RecordNotFound",
				"message": fmt.Sprintf("could not locate record: %s", uri),
			})
		}
		recordCID = repost.CID
		recordRaw = repost.Raw

	default:
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": fmt.Sprintf("unsupported collection: %s", collection),
		})
	}

	// Check CID if provided
	if cidParam != "" && recordCID != cidParam {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "RecordNotFound",
			"message": fmt.Sprintf("could not locate record: %s", uri),
		})
	}

	// Decode the CBOR record
	// For now, return a placeholder - full CBOR decoding would require
	// type-specific unmarshalers for each collection type
	var value interface{}
	if len(recordRaw) > 0 {
		rec, err := lexutil.CborDecodeValue(recordRaw)
		if err != nil {
			return err
		}

		value = rec
	}

	// Suppress unused import warning
	_ = cbg.CborNull

	return c.JSON(http.StatusOK, map[string]interface{}{
		"uri":   uri,
		"cid":   recordCID,
		"value": value,
	})
}

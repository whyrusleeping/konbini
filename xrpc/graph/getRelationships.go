package graph

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"gorm.io/gorm"
)

// HandleGetRelationships implements app.bsky.graph.getRelationships
func HandleGetRelationships(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	actorParam := c.QueryParam("actor")
	if actorParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "actor parameter is required",
		})
	}

	// Parse others parameter (can be multiple)
	others := c.QueryParams()["others"]
	if len(others) == 0 {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"actor":         actorParam,
			"relationships": []interface{}{},
		})
	}

	// Limit to reasonable batch size
	if len(others) > 30 {
		others = others[:30]
	}

	ctx := c.Request().Context()

	// Resolve actor to DID
	actorDID, err := hydrator.ResolveDID(ctx, actorParam)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "ActorNotFound",
			"message": "actor not found",
		})
	}

	// Build relationships for each "other" actor
	relationships := make([]interface{}, 0, len(others))

	for _, other := range others {
		// Resolve other to DID
		otherDID, err := hydrator.ResolveDID(ctx, other)
		if err != nil {
			// Actor not found
			relationships = append(relationships, map[string]interface{}{
				"$type":    "app.bsky.graph.defs#notFoundActor",
				"actor":    other,
				"notFound": true,
			})
			continue
		}

		// Check if actor follows other
		var following string
		err = db.Raw(`
			SELECT 'at://' || r1.did || '/app.bsky.graph.follow/' || f.rkey as uri
			FROM follows f
			JOIN repos r1 ON r1.id = f.author
			JOIN repos r2 ON r2.id = f.subject
			WHERE r1.did = ? AND r2.did = ?
			LIMIT 1
		`, actorDID, otherDID).Scan(&following).Error
		if err != nil {
			following = ""
		}

		// Check if other follows actor
		var followedBy string
		err = db.Raw(`
			SELECT 'at://' || r1.did || '/app.bsky.graph.follow/' || f.rkey as uri
			FROM follows f
			JOIN repos r1 ON r1.id = f.author
			JOIN repos r2 ON r2.id = f.subject
			WHERE r1.did = ? AND r2.did = ?
			LIMIT 1
		`, otherDID, actorDID).Scan(&followedBy).Error
		if err != nil {
			followedBy = ""
		}

		relationships = append(relationships, map[string]interface{}{
			"$type":      "app.bsky.graph.defs#relationship",
			"did":        otherDID,
			"following":  following,
			"followedBy": followedBy,
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"actor":         actorDID,
		"relationships": relationships,
	})
}

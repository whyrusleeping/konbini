package graph

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetFollowers implements app.bsky.graph.getFollowers
func HandleGetFollowers(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	actorParam := c.QueryParam("actor")
	if actorParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "actor parameter is required",
		})
	}

	// Parse limit
	limit := 50
	if limitParam := c.QueryParam("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	// Parse cursor (follow ID)
	var cursor uint
	if cursorParam := c.QueryParam("cursor"); cursorParam != "" {
		if c, err := strconv.ParseUint(cursorParam, 10, 64); err == nil {
			cursor = uint(c)
		}
	}

	ctx := c.Request().Context()

	// Resolve actor to DID
	did, err := hydrator.ResolveDID(ctx, actorParam)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "ActorNotFound",
			"message": "actor not found",
		})
	}

	// Get the subject actor info
	subjectInfo, err := hydrator.HydrateActor(ctx, did)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "ActorNotFound",
			"message": "failed to load actor",
		})
	}

	// Query followers
	type followerRow struct {
		ID        uint
		AuthorDid string
	}
	var rows []followerRow

	query := `
		SELECT f.id, r.did as author_did
		FROM follows f
		JOIN repos r ON r.id = f.author
		WHERE f.subject = (SELECT id FROM repos WHERE did = ?)
	`
	if cursor > 0 {
		query += ` AND f.id < ?`
	}
	query += ` ORDER BY f.id DESC LIMIT ?`

	var queryArgs []interface{}
	queryArgs = append(queryArgs, did)
	if cursor > 0 {
		queryArgs = append(queryArgs, cursor)
	}
	queryArgs = append(queryArgs, limit)

	if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to query followers",
		})
	}

	// Hydrate follower actors
	followers := make([]interface{}, 0)
	for _, row := range rows {
		actorInfo, err := hydrator.HydrateActor(ctx, row.AuthorDid)
		if err != nil {
			continue
		}
		followers = append(followers, views.ProfileView(actorInfo))
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		nextCursor = strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"subject":   views.ProfileView(subjectInfo),
		"followers": followers,
		"cursor":    nextCursor,
	})
}

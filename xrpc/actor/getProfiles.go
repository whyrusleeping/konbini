package actor

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleGetProfiles implements app.bsky.actor.getProfiles
func HandleGetProfiles(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	// Parse actors parameter (can be multiple)
	actors := c.QueryParams()["actors"]
	if len(actors) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "actors parameter is required",
		})
	}

	// Limit to reasonable batch size
	if len(actors) > 25 {
		actors = actors[:25]
	}

	ctx := c.Request().Context()

	// Resolve all actors to DIDs and hydrate profiles
	profiles := make([]interface{}, 0)
	for _, actor := range actors {
		// Resolve actor to DID
		did, err := hydrator.ResolveDID(ctx, actor)
		if err != nil {
			// Skip actors that can't be resolved
			continue
		}

		// Hydrate actor info
		actorInfo, err := hydrator.HydrateActor(ctx, did)
		if err != nil {
			// Skip actors that can't be hydrated
			continue
		}

		// Get counts for the profile
		type counts struct {
			Followers int
			Follows   int
			Posts     int
		}
		var c counts
		db.Raw(`
			SELECT
				(SELECT COUNT(*) FROM follows WHERE subject = (SELECT id FROM repos WHERE did = ?)) as followers,
				(SELECT COUNT(*) FROM follows WHERE author = (SELECT id FROM repos WHERE did = ?)) as follows,
				(SELECT COUNT(*) FROM posts WHERE author = (SELECT id FROM repos WHERE did = ?)) as posts
		`, did, did, did).Scan(&c)

		profiles = append(profiles, views.ProfileViewDetailed(actorInfo, c.Followers, c.Follows, c.Posts))
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"profiles": profiles,
	})
}

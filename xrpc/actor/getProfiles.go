package actor

import (
	"net/http"

	"github.com/bluesky-social/indigo/api/bsky"
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
	viewer, _ := c.Get("viewer").(string)

	// Resolve all actors to DIDs and hydrate profiles
	profiles := make([]*bsky.ActorDefs_ProfileViewDetailed, 0, len(actors))
	for _, actor := range actors {
		// Resolve actor to DID
		did, err := hydrator.ResolveDID(ctx, actor)
		if err != nil {
			// Skip actors that can't be resolved
			continue
		}

		// Hydrate actor info
		actorInfo, err := hydrator.HydrateActorDetailed(ctx, did, viewer)
		if err != nil {
			// Skip actors that can't be hydrated
			continue
		}

		profiles = append(profiles, views.ProfileViewDetailed(actorInfo))
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"profiles": profiles,
	})
}

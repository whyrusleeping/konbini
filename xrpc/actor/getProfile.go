package actor

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
)

// HandleGetProfile implements app.bsky.actor.getProfile
func HandleGetProfile(c echo.Context, hydrator *hydration.Hydrator) error {
	actorParam := c.QueryParam("actor")
	if actorParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "actor parameter is required",
		})
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

	// Hydrate actor info
	actorInfo, err := hydrator.HydrateActorDetailed(ctx, did)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]interface{}{
			"error":   "ActorNotFound",
			"message": "failed to load actor",
		})
	}

	// Build response
	profile := views.ProfileViewDetailed(actorInfo)

	return c.JSON(http.StatusOK, profile)
}

package feed

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
)

// HandleGetPosts implements app.bsky.feed.getPosts
func HandleGetPosts(c echo.Context, hydrator *hydration.Hydrator) error {
	// Get URIs from query params (can be multiple)
	urisParam := c.QueryParam("uris")
	if urisParam == "" {
		return c.JSON(http.StatusBadRequest, map[string]interface{}{
			"error":   "InvalidRequest",
			"message": "uris parameter is required",
		})
	}

	// Parse URIs (they come as a comma-separated list or as multiple query params)
	var uris []string
	if strings.Contains(urisParam, ",") {
		uris = strings.Split(urisParam, ",")
	} else {
		// Check for multiple uri query params
		uris = c.QueryParams()["uris"]
		if len(uris) == 0 {
			uris = []string{urisParam}
		}
	}

	// Limit to reasonable number
	if len(uris) > 25 {
		uris = uris[:25]
	}

	ctx := c.Request().Context()
	viewer := getUserDID(c)

	// Hydrate posts
	postsMap, err := hydrator.HydratePosts(ctx, uris, viewer)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to load posts",
		})
	}

	// Build response - need to maintain order of requested URIs
	posts := make([]interface{}, 0)
	for _, uri := range uris {
		postInfo, ok := postsMap[uri]
		if !ok {
			// Post not found, skip it
			continue
		}

		// Hydrate author
		authorInfo, err := hydrator.HydrateActor(ctx, postInfo.Author)
		if err != nil {
			continue
		}

		postView := views.PostView(postInfo, authorInfo)
		posts = append(posts, postView)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"posts": posts,
	})
}

func getUserDID(c echo.Context) string {
	did := c.Get("viewer")
	if did == nil {
		return ""
	}
	if s, ok := did.(string); ok {
		return s
	}
	return ""
}

package actor

import (
	"net/http"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"gorm.io/gorm"
)

// HandleGetPreferences implements app.bsky.actor.getPreferences
// This is typically a PDS endpoint, not an AppView endpoint.
// For now, return empty preferences.
func HandleGetPreferences(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	// Get viewer from authentication
	viewer := c.Get("viewer")
	if viewer == nil {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	out := bsky.ActorGetPreferences_Output{
		Preferences: []bsky.ActorDefs_Preferences_Elem{
			{
				ActorDefs_AdultContentPref: &bsky.ActorDefs_AdultContentPref{
					Enabled: true,
				},
			},
			{
				ActorDefs_ContentLabelPref: &bsky.ActorDefs_ContentLabelPref{
					Label:      "nsfw",
					Visibility: "warn",
				},
			},
			/*
				{
					ActorDefs_LabelersPref: &bsky.ActorDefs_LabelersPref{
						Labelers: []*bsky.ActorDefs_LabelerPrefItem{},
					},
				},
			*/
			{
				ActorDefs_BskyAppStatePref: &bsky.ActorDefs_BskyAppStatePref{
					Nuxs: []*bsky.ActorDefs_Nux{
						{
							Id:        "NeueTypography",
							Completed: true,
						},
						{
							Id:        "PolicyUpdate202508",
							Completed: true,
						},
					},
				},
			},
			{
				ActorDefs_SavedFeedsPrefV2: &bsky.ActorDefs_SavedFeedsPrefV2{
					Items: []*bsky.ActorDefs_SavedFeed{
						{
							Id:     "3m2k6cbfsq22n",
							Pinned: true,
							Type:   "timeline",
							Value:  "following",
						},
					},
				},
			},
		},
	}

	return c.JSON(http.StatusOK, out)
}

/*
{
            "nuxs": [
                {
                    "id": "TenMillionDialog",
                    "completed": true
                },
                {
                    "id": "NeueTypography",
                    "completed": true
                },
                {
                    "id": "NeueChar",
                    "completed": true
                },
                {
                    "id": "InitialVerificationAnnouncement",
                    "completed": true
                },
                {
                    "id": "ActivitySubscriptions",
                    "completed": true
                },
                {
                    "id": "BookmarksAnnouncement",
                    "completed": true
                },
                {
                    "id": "PolicyUpdate202508",
                    "completed": true
                }
            ],
            "$type": "app.bsky.actor.defs#bskyAppStatePref"
        }
*/

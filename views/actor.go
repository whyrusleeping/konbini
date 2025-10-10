package views

import (
	"fmt"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/whyrusleeping/konbini/hydration"
)

// ProfileViewBasic builds a basic profile view (app.bsky.actor.defs#profileViewBasic)
func ProfileViewBasic(actor *hydration.ActorInfo) *bsky.ActorDefs_ProfileViewBasic {
	view := &bsky.ActorDefs_ProfileViewBasic{
		Did:    actor.DID,
		Handle: actor.Handle,
	}

	if actor.Profile != nil {
		if actor.Profile.DisplayName != nil && *actor.Profile.DisplayName != "" {
			view.DisplayName = actor.Profile.DisplayName
		}
		if actor.Profile.Avatar != nil {
			avatarURL := formatBlobRef(actor.DID, actor.Profile.Avatar)
			if avatarURL != "" {
				view.Avatar = &avatarURL
			}
		}
	}

	return view
}

// ProfileView builds a profile view (app.bsky.actor.defs#profileView)
func ProfileView(actor *hydration.ActorInfo) *bsky.ActorDefs_ProfileView {
	view := &bsky.ActorDefs_ProfileView{
		Did:    actor.DID,
		Handle: actor.Handle,
	}

	if actor.Profile != nil {
		if actor.Profile.DisplayName != nil && *actor.Profile.DisplayName != "" {
			view.DisplayName = actor.Profile.DisplayName
		}
		if actor.Profile.Description != nil && *actor.Profile.Description != "" {
			view.Description = actor.Profile.Description
		}
		if actor.Profile.Avatar != nil {
			avatarURL := formatBlobRef(actor.DID, actor.Profile.Avatar)
			if avatarURL != "" {
				view.Avatar = &avatarURL
			}
		}
		// Note: CreatedAt is typically set on the profile record itself
	}

	return view
}

// ProfileViewDetailed builds a detailed profile view (app.bsky.actor.defs#profileViewDetailed)
func ProfileViewDetailed(actor *hydration.ActorInfoDetailed) *bsky.ActorDefs_ProfileViewDetailed {
	view := &bsky.ActorDefs_ProfileViewDetailed{
		Did:    actor.DID,
		Handle: actor.Handle,
	}

	if actor.Profile != nil {
		if actor.Profile.DisplayName != nil && *actor.Profile.DisplayName != "" {
			view.DisplayName = actor.Profile.DisplayName
		}
		if actor.Profile.Description != nil && *actor.Profile.Description != "" {
			view.Description = actor.Profile.Description
		}
		if actor.Profile.Avatar != nil {
			avatarURL := formatBlobRef(actor.DID, actor.Profile.Avatar)
			if avatarURL != "" {
				view.Avatar = &avatarURL
			}
		}
		if actor.Profile.Banner != nil {
			bannerURL := formatBlobRef(actor.DID, actor.Profile.Banner)
			if bannerURL != "" {
				view.Banner = &bannerURL
			}
		}
	}

	// Add counts
	view.FollowersCount = &actor.FollowerCount
	view.FollowsCount = &actor.FollowCount
	view.PostsCount = &actor.PostCount

	// Add viewer state if available
	if actor.ViewerState != nil {
		view.Viewer = actor.ViewerState
	}

	return view
}

func formatBlobRef(did string, blob *util.LexBlob) string {
	return fmt.Sprintf("https://cdn.bsky.app/img/avatar_thumbnail/plain/%s/%s@jpeg", did, blob.Ref.String())
}

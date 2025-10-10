package hydration

import (
	"github.com/bluesky-social/indigo/atproto/identity"
	"gorm.io/gorm"
)

// Hydrator handles data hydration from the database
type Hydrator struct {
	db  *gorm.DB
	dir identity.Directory

	missingRecordCallback func(string, bool)
}

// NewHydrator creates a new Hydrator
func NewHydrator(db *gorm.DB, dir identity.Directory) *Hydrator {
	return &Hydrator{
		db:  db,
		dir: dir,
	}
}

// SetMissingRecordCallback sets the callback for when a record is missing
// The callback receives an identifier which can be:
// - A DID (e.g., "did:plc:...") for actors/profiles
// - An AT-URI (e.g., "at://did:plc:.../app.bsky.feed.post/...") for posts
// - An AT-URI (e.g., "at://did:plc:.../app.bsky.feed.generator/...") for feed generators
func (h *Hydrator) SetMissingRecordCallback(fn func(string, bool)) {
	h.missingRecordCallback = fn
}

// AddMissingRecord reports a missing record that needs to be fetched
func (h *Hydrator) AddMissingRecord(identifier string, wait bool) {
	if h.missingRecordCallback != nil {
		h.missingRecordCallback(identifier, wait)
	}
}

// addMissingActor is a convenience method for adding missing actors
func (h *Hydrator) addMissingActor(did string) {
	h.AddMissingRecord(did, false)
}

// HydrateCtx contains context for hydration operations
type HydrateCtx struct {
	Viewer string
}

// NewHydrateCtx creates a new hydration context
func NewHydrateCtx(viewer string) *HydrateCtx {
	return &HydrateCtx{
		Viewer: viewer,
	}
}

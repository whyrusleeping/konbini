package hydration

import (
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/whyrusleeping/konbini/backend"
	"gorm.io/gorm"
)

// Hydrator handles data hydration from the database
type Hydrator struct {
	db      *gorm.DB
	dir     identity.Directory
	backend *backend.PostgresBackend
}

// NewHydrator creates a new Hydrator
func NewHydrator(db *gorm.DB, dir identity.Directory, backend *backend.PostgresBackend) *Hydrator {
	return &Hydrator{
		db:      db,
		dir:     dir,
		backend: backend,
	}
}

// AddMissingRecord reports a missing record that needs to be fetched
func (h *Hydrator) AddMissingRecord(identifier string, wait bool) {
	if h.backend != nil {
		h.backend.TrackMissingRecord(identifier, wait)
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

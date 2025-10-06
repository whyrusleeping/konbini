package hydration

import (
	"github.com/bluesky-social/indigo/atproto/identity"
	"gorm.io/gorm"
)

// Hydrator handles data hydration from the database
type Hydrator struct {
	db  *gorm.DB
	dir identity.Directory

	missingActorCallback func(string)
	missingPostCallback  func(string)
}

// NewHydrator creates a new Hydrator
func NewHydrator(db *gorm.DB, dir identity.Directory) *Hydrator {
	return &Hydrator{
		db:  db,
		dir: dir,
	}
}

func (h *Hydrator) SetMissingActorCallback(fn func(string)) {
	h.missingActorCallback = fn
}

func (h *Hydrator) addMissingActor(did string) {
	if h.missingActorCallback != nil {
		h.missingActorCallback(did)
	}
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

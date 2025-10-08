package hydration

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

func (h *Hydrator) NormalizeUri(ctx context.Context, uri string) (string, error) {
	puri, err := syntax.ParseATURI(uri)
	if err != nil {
		return "", fmt.Errorf("invalid uri: %w", err)
	}

	var did string
	if !puri.Authority().IsDID() {
		resp, err := h.dir.LookupHandle(ctx, syntax.Handle(puri.Authority().String()))
		if err != nil {
			return "", err
		}

		did = resp.DID.String()
	} else {
		did = puri.Authority().String()
	}

	return fmt.Sprintf("at://%s/%s/%s", did, puri.Collection().String(), puri.RecordKey().String()), nil
}

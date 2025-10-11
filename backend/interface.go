package backend

// RecordTracker is an interface for tracking missing records that need to be fetched
type RecordTracker interface {
	// TrackMissingRecord queues a missing record for fetching
	// identifier can be:
	//  - A DID (e.g., "did:plc:...") for actors/profiles
	//  - An AT-URI (e.g., "at://did:plc:.../app.bsky.feed.post/...") for posts
	//  - An AT-URI (e.g., "at://did:plc:.../app.bsky.feed.generator/...") for feed generators
	// wait: if true, blocks until the record is fetched
	TrackMissingRecord(identifier string, wait bool)
}

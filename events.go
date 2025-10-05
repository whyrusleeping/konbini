package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

type PostgresBackend struct {
	db  *gorm.DB
	pgx *pgxpool.Pool
	s   *Server

	relevantDids map[string]bool
	rdLk         sync.Mutex

	revCache *lru.TwoQueueCache[uint, string]

	repoCache *lru.TwoQueueCache[string, *Repo]
	reposLk   sync.Mutex

	postInfoCache *lru.TwoQueueCache[string, cachedPostInfo]
}

func (b *PostgresBackend) HandleEvent(ctx context.Context, evt *atproto.SyncSubscribeRepos_Commit) error {
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
	if err != nil {
		return fmt.Errorf("failed to read event repo: %w", err)
	}

	for _, op := range evt.Ops {
		switch op.Action {
		case "create":
			c, rec, err := r.GetRecordBytes(ctx, op.Path)
			if err != nil {
				return err
			}
			if err := b.HandleCreate(ctx, evt.Repo, evt.Rev, op.Path, rec, &c); err != nil {
				return fmt.Errorf("create record failed: %w", err)
			}
		case "update":
			c, rec, err := r.GetRecordBytes(ctx, op.Path)
			if err != nil {
				return err
			}
			if err := b.HandleUpdate(ctx, evt.Repo, evt.Rev, op.Path, rec, &c); err != nil {
				return fmt.Errorf("update record failed: %w", err)
			}
		case "delete":
			if err := b.HandleDelete(ctx, evt.Repo, evt.Rev, op.Path); err != nil {
				return fmt.Errorf("delete record failed: %w", err)
			}
		}
	}

	// TODO: sync with the Since field to make sure we don't miss events we care about
	/*
		if err := bf.Store.UpdateRev(ctx, evt.Repo, evt.Rev); err != nil {
			return fmt.Errorf("failed to update rev: %w", err)
		}
	*/

	return nil
}

func (b *PostgresBackend) HandleCreate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	start := time.Now()

	rr, err := b.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	lrev, err := b.revForRepo(rr)
	if err != nil {
		return err
	}
	if lrev != "" {
		if rev < lrev {
			slog.Info("skipping old rev create", "did", rr.Did, "rev", rev, "oldrev", lrev, "path", path)
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleCreate: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("create", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	if rkey == "" {
		fmt.Printf("messed up path: %q\n", rkey)
	}

	switch col {
	case "app.bsky.feed.post":
		if err := b.HandleCreatePost(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.like":
		if err := b.HandleCreateLike(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.repost":
		if err := b.HandleCreateRepost(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.follow":
		if err := b.HandleCreateFollow(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.block":
		if err := b.HandleCreateBlock(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.list":
		if err := b.HandleCreateList(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.listitem":
		if err := b.HandleCreateListitem(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.listblock":
		if err := b.HandleCreateListblock(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.actor.profile":
		if err := b.HandleCreateProfile(ctx, rr, rkey, rev, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.generator":
		if err := b.HandleCreateFeedGenerator(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.threadgate":
		if err := b.HandleCreateThreadgate(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "chat.bsky.actor.declaration":
		if err := b.HandleCreateChatDeclaration(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.feed.postgate":
		if err := b.HandleCreatePostGate(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	case "app.bsky.graph.starterpack":
		if err := b.HandleCreateStarterPack(ctx, rr, rkey, *rec, *cid); err != nil {
			return err
		}
	default:
		slog.Debug("unrecognized record type", "repo", repo, "path", path, "rev", rev)
	}

	b.revCache.Add(rr.ID, rev)
	return nil
}

func (b *PostgresBackend) HandleCreatePost(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	exists, err := b.checkPostExists(ctx, repo, rkey)
	if err != nil {
		return err
	}

	// still technically a race condition if two creates for the same post happen concurrently... probably fine
	if exists {
		return nil
	}

	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	reldids := []string{repo.Did}
	// care about a post if its in a thread of a user we are interested in
	if rec.Reply != nil && rec.Reply.Parent != nil && rec.Reply.Root != nil {
		reldids = append(reldids, rec.Reply.Parent.Uri, rec.Reply.Root.Uri)
	}
	// TODO: maybe also care if its mentioning a user we care about or quoting a user we care about?
	if !b.anyRelevantIdents(reldids...) {
		return nil
	}

	uri := "at://" + repo.Did + "/app.bsky.feed.post/" + rkey
	slog.Warn("adding post", "uri", uri)

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	p := Post{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Raw:     recb,
		Cid:     cc.String(),
	}

	if rec.Reply != nil && rec.Reply.Parent != nil {
		if rec.Reply.Root == nil {
			return fmt.Errorf("post reply had nil root")
		}

		pinfo, err := b.postInfoForUri(ctx, rec.Reply.Parent.Uri)
		if err != nil {
			return fmt.Errorf("getting reply parent: %w", err)
		}

		p.ReplyTo = pinfo.ID
		p.ReplyToUsr = pinfo.Author

		thread, err := b.postIDForUri(ctx, rec.Reply.Root.Uri)
		if err != nil {
			return fmt.Errorf("getting thread root: %w", err)
		}

		p.InThread = thread

		if p.ReplyToUsr == b.s.myrepo.ID {
			if err := b.s.AddNotification(ctx, b.s.myrepo.ID, p.Author, uri, NotifKindReply); err != nil {
				slog.Warn("failed to create notification", "uri", uri, "error", err)
			}
		}
	}

	if rec.Embed != nil {
		var rpref string
		if rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
			rpref = rec.Embed.EmbedRecord.Record.Uri
		}
		if rec.Embed.EmbedRecordWithMedia != nil &&
			rec.Embed.EmbedRecordWithMedia.Record != nil &&
			rec.Embed.EmbedRecordWithMedia.Record.Record != nil {
			rpref = rec.Embed.EmbedRecordWithMedia.Record.Record.Uri
		}

		if rpref != "" && strings.Contains(rpref, "app.bsky.feed.post") {
			rp, err := b.postIDForUri(ctx, rpref)
			if err != nil {
				return fmt.Errorf("getting quote subject: %w", err)
			}

			p.Reposting = rp
		}
	}

	if err := b.doPostCreate(ctx, &p); err != nil {
		return err
	}

	// Check for mentions and create notifications
	if rec.Facets != nil {
		for _, facet := range rec.Facets {
			for _, feature := range facet.Features {
				if feature.RichtextFacet_Mention != nil {
					mentionDid := feature.RichtextFacet_Mention.Did
					// This is a mention
					mentionedRepo, err := b.getOrCreateRepo(ctx, mentionDid)
					if err != nil {
						slog.Warn("failed to get repo for mention", "did", mentionDid, "error", err)
						continue
					}

					// Create notification if the mentioned user is the current user
					if mentionedRepo.ID == b.s.myrepo.ID {
						if err := b.s.AddNotification(ctx, b.s.myrepo.ID, p.Author, uri, NotifKindMention); err != nil {
							slog.Warn("failed to create mention notification", "uri", uri, "error", err)
						}
					}
				}
			}
		}
	}

	b.postInfoCache.Add(uri, cachedPostInfo{
		ID:     p.ID,
		Author: p.Author,
	})

	return nil
}

func (b *PostgresBackend) doPostCreate(ctx context.Context, p *Post) error {
	/*
		if err := b.db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "author"}, {Name: "rkey"}},
			DoUpdates: clause.AssignmentColumns([]string{"cid", "not_found", "raw", "created", "indexed"}),
		}).Create(p).Error; err != nil {
			return err
		}
	*/

	query := `
INSERT INTO posts (author, rkey, cid, not_found, raw, created, indexed, reposting, reply_to, reply_to_usr, in_thread) 
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (author, rkey) 
DO UPDATE SET 
    cid = $3,
    not_found = $4,
    raw = $5,
    created = $6,
    indexed = $7,
    reposting = $8,
    reply_to = $9,
    reply_to_usr = $10,
    in_thread = $11
RETURNING id
`

	// Execute the query with parameters from the Post struct
	if err := b.pgx.QueryRow(
		ctx,
		query,
		p.Author,
		p.Rkey,
		p.Cid,
		p.NotFound,
		p.Raw,
		p.Created,
		p.Indexed,
		p.Reposting,
		p.ReplyTo,
		p.ReplyToUsr,
		p.InThread,
	).Scan(&p.ID); err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateLike(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedLike
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	if !b.anyRelevantIdents(repo.Did, rec.Subject.Uri) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	pinfo, err := b.postInfoForUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting like subject: %w", err)
	}

	if _, err := b.pgx.Exec(ctx, `INSERT INTO "likes" ("created","indexed","author","rkey","subject","cid") VALUES ($1, $2, $3, $4, $5, $6)`, created.Time(), time.Now(), repo.ID, rkey, pinfo.ID, cc.String()); err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Code == "23505" {
			return nil
		}
		return err
	}

	// Create notification if the liked post belongs to the current user
	if pinfo.Author == b.s.myrepo.ID {
		uri := fmt.Sprintf("at://%s/app.bsky.feed.like/%s", repo.Did, rkey)
		if err := b.s.AddNotification(ctx, b.s.myrepo.ID, repo.ID, uri, NotifKindLike); err != nil {
			slog.Warn("failed to create like notification", "uri", uri, "error", err)
		}
	}

	return nil
}

func (b *PostgresBackend) HandleCreateRepost(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.FeedRepost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	if !b.anyRelevantIdents(repo.Did, rec.Subject.Uri) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	pinfo, err := b.postInfoForUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting repost subject: %w", err)
	}

	if _, err := b.pgx.Exec(ctx, `INSERT INTO "reposts" ("created","indexed","author","rkey","subject") VALUES ($1, $2, $3, $4, $5)`, created.Time(), time.Now(), repo.ID, rkey, pinfo.ID); err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Code == "23505" {
			return nil
		}
		return err
	}

	// Create notification if the reposted post belongs to the current user
	if pinfo.Author == b.s.myrepo.ID {
		uri := fmt.Sprintf("at://%s/app.bsky.feed.repost/%s", repo.Did, rkey)
		if err := b.s.AddNotification(ctx, b.s.myrepo.ID, repo.ID, uri, NotifKindRepost); err != nil {
			slog.Warn("failed to create repost notification", "uri", uri, "error", err)
		}
	}

	return nil
}

func (b *PostgresBackend) HandleCreateFollow(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphFollow
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	if !b.anyRelevantIdents(repo.Did, rec.Subject) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subj, err := b.getOrCreateRepo(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if _, err := b.pgx.Exec(ctx, "INSERT INTO follows (created, indexed, author, rkey, subject) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING", created.Time(), time.Now(), repo.ID, rkey, subj.ID); err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateBlock(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphBlock
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	if !b.anyRelevantIdents(repo.Did, rec.Subject) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subj, err := b.getOrCreateRepo(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if err := b.db.Create(&Block{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: subj.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateList(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphList
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	if err := b.db.Create(&List{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Raw:     recb,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateListitem(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphListitem
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	subj, err := b.getOrCreateRepo(ctx, rec.Subject)
	if err != nil {
		return err
	}

	list, err := b.getOrCreateList(ctx, rec.List)
	if err != nil {
		return err
	}

	if err := b.db.Create(&ListItem{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: subj.ID,
		List:    list.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateListblock(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	var rec bsky.GraphListblock
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	if !b.anyRelevantIdents(repo.Did, rec.Subject) {
		return nil
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	list, err := b.getOrCreateList(ctx, rec.Subject)
	if err != nil {
		return err
	}

	if err := b.db.Create(&ListBlock{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		List:    list.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateProfile(ctx context.Context, repo *Repo, rkey, rev string, recb []byte, cc cid.Cid) error {
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}

	if err := b.db.Create(&Profile{
		//Created: created.Time(),
		Indexed: time.Now(),
		Repo:    repo.ID,
		Raw:     recb,
		Rev:     rev,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleUpdateProfile(ctx context.Context, repo *Repo, rkey, rev string, recb []byte, cc cid.Cid) error {
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}

	if err := b.db.Create(&Profile{
		Indexed: time.Now(),
		Repo:    repo.ID,
		Raw:     recb,
		Rev:     rev,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateFeedGenerator(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}

	var rec bsky.FeedGenerator
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	if err := b.db.Create(&FeedGenerator{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Did:     rec.Did,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateThreadgate(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}
	var rec bsky.FeedThreadgate
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	pid, err := b.postIDForUri(ctx, rec.Post)
	if err != nil {
		return err
	}

	if err := b.db.Create(&ThreadGate{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Post:    pid,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateChatDeclaration(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	// TODO: maybe track these?
	return nil
}

func (b *PostgresBackend) HandleCreatePostGate(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}
	var rec bsky.FeedPostgate
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	refPost, err := b.postInfoForUri(ctx, rec.Post)
	if err != nil {
		return err
	}

	if err := b.db.Create(&PostGate{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Subject: refPost.ID,
		Raw:     recb,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleCreateStarterPack(ctx context.Context, repo *Repo, rkey string, recb []byte, cc cid.Cid) error {
	if !b.anyRelevantIdents(repo.Did) {
		return nil
	}
	var rec bsky.GraphStarterpack
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}
	created, err := syntax.ParseDatetimeLenient(rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	list, err := b.getOrCreateList(ctx, rec.List)
	if err != nil {
		return err
	}

	if err := b.db.Create(&StarterPack{
		Created: created.Time(),
		Indexed: time.Now(),
		Author:  repo.ID,
		Rkey:    rkey,
		Raw:     recb,
		List:    list.ID,
	}).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleUpdate(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid) error {
	start := time.Now()

	rr, err := b.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	lrev, err := b.revForRepo(rr)
	if err != nil {
		return err
	}
	if lrev != "" {
		if rev < lrev {
			//slog.Info("skipping old rev create", "did", rr.Did, "rev", rev, "oldrev", lrev, "path", path)
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleCreate: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("update", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	if rkey == "" {
		fmt.Printf("messed up path: %q\n", rkey)
	}

	switch col {
	/*
		case "app.bsky.feed.post":
			if err := s.HandleCreatePost(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.feed.like":
			if err := s.HandleCreateLike(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.feed.repost":
			if err := s.HandleCreateRepost(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.graph.follow":
			if err := s.HandleCreateFollow(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.graph.block":
			if err := s.HandleCreateBlock(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.graph.list":
			if err := s.HandleCreateList(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.graph.listitem":
			if err := s.HandleCreateListitem(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
		case "app.bsky.graph.listblock":
			if err := s.HandleCreateListblock(ctx, rr, rkey, *rec, *cid); err != nil {
				return err
			}
	*/
	case "app.bsky.actor.profile":
		if err := b.HandleUpdateProfile(ctx, rr, rkey, rev, *rec, *cid); err != nil {
			return err
		}
		/*
			case "app.bsky.feed.generator":
				if err := s.HandleCreateFeedGenerator(ctx, rr, rkey, *rec, *cid); err != nil {
					return err
				}
			case "app.bsky.feed.threadgate":
				if err := s.HandleCreateThreadgate(ctx, rr, rkey, *rec, *cid); err != nil {
					return err
				}
			case "chat.bsky.actor.declaration":
				if err := s.HandleCreateChatDeclaration(ctx, rr, rkey, *rec, *cid); err != nil {
					return err
				}
		*/
	default:
		slog.Debug("unrecognized record type in update", "repo", repo, "path", path, "rev", rev)
	}

	return nil
}

func (b *PostgresBackend) HandleDelete(ctx context.Context, repo string, rev string, path string) error {
	start := time.Now()

	rr, err := b.getOrCreateRepo(ctx, repo)
	if err != nil {
		return fmt.Errorf("get user failed: %w", err)
	}

	lrev, ok := b.revCache.Get(rr.ID)
	if ok {
		if rev < lrev {
			//slog.Info("skipping old rev delete", "did", rr.Did, "rev", rev, "oldrev", lrev)
			return nil
		}
	}

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid path in HandleDelete: %q", path)
	}
	col := parts[0]
	rkey := parts[1]

	defer func() {
		handleOpHist.WithLabelValues("create", col).Observe(float64(time.Since(start).Milliseconds()))
	}()

	switch col {
	case "app.bsky.feed.post":
		if err := b.HandleDeletePost(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.like":
		if err := b.HandleDeleteLike(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.repost":
		if err := b.HandleDeleteRepost(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.follow":
		if err := b.HandleDeleteFollow(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.block":
		if err := b.HandleDeleteBlock(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.list":
		if err := b.HandleDeleteList(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.listitem":
		if err := b.HandleDeleteListitem(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.graph.listblock":
		if err := b.HandleDeleteListblock(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.actor.profile":
		if err := b.HandleDeleteProfile(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.generator":
		if err := b.HandleDeleteFeedGenerator(ctx, rr, rkey); err != nil {
			return err
		}
	case "app.bsky.feed.threadgate":
		if err := b.HandleDeleteThreadgate(ctx, rr, rkey); err != nil {
			return err
		}
	default:
		slog.Warn("delete unrecognized record type", "repo", repo, "path", path, "rev", rev)
	}

	b.revCache.Add(rr.ID, rev)
	return nil
}

func (b *PostgresBackend) HandleDeletePost(ctx context.Context, repo *Repo, rkey string) error {
	var p Post
	if err := b.db.Find(&p, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if p.ID == 0 {
		//slog.Warn("delete of unknown post record", "repo", repo.Did, "rkey", rkey)
		return nil
	}

	if err := b.db.Delete(&Post{}, p.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteLike(ctx context.Context, repo *Repo, rkey string) error {
	var like Like
	if err := b.db.Find(&like, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if like.ID == 0 {
		//slog.Warn("delete of missing like", "repo", repo.Did, "rkey", rkey)
		return nil
	}

	if err := b.db.Exec("DELETE FROM likes WHERE id = ?", like.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteRepost(ctx context.Context, repo *Repo, rkey string) error {
	var repost Repost
	if err := b.db.Find(&repost, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if repost.ID == 0 {
		//return fmt.Errorf("delete of missing repost: %s %s", repo.Did, rkey)
		return nil
	}

	if err := b.db.Exec("DELETE FROM reposts WHERE id = ?", repost.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteFollow(ctx context.Context, repo *Repo, rkey string) error {
	var follow Follow
	if err := b.db.Find(&follow, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if follow.ID == 0 {
		//slog.Warn("delete of missing follow", "repo", repo.Did, "rkey", rkey)
		return nil
	}

	if err := b.db.Exec("DELETE FROM follows WHERE id = ?", follow.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteBlock(ctx context.Context, repo *Repo, rkey string) error {
	var block Block
	if err := b.db.Find(&block, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if block.ID == 0 {
		//slog.Warn("delete of missing block", "repo", repo.Did, "rkey", rkey)
		return nil
	}

	if err := b.db.Exec("DELETE FROM blocks WHERE id = ?", block.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteList(ctx context.Context, repo *Repo, rkey string) error {
	var list List
	if err := b.db.Find(&list, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if list.ID == 0 {
		return nil
		//return fmt.Errorf("delete of missing list: %s %s", repo.Did, rkey)
	}

	if err := b.db.Exec("DELETE FROM lists WHERE id = ?", list.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteListitem(ctx context.Context, repo *Repo, rkey string) error {
	var item ListItem
	if err := b.db.Find(&item, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if item.ID == 0 {
		return nil
		//return fmt.Errorf("delete of missing listitem: %s %s", repo.Did, rkey)
	}

	if err := b.db.Exec("DELETE FROM list_items WHERE id = ?", item.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteListblock(ctx context.Context, repo *Repo, rkey string) error {
	var block ListBlock
	if err := b.db.Find(&block, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if block.ID == 0 {
		return nil
		//return fmt.Errorf("delete of missing listblock: %s %s", repo.Did, rkey)
	}

	if err := b.db.Exec("DELETE FROM list_blocks WHERE id = ?", block.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteFeedGenerator(ctx context.Context, repo *Repo, rkey string) error {
	var feedgen FeedGenerator
	if err := b.db.Find(&feedgen, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if feedgen.ID == 0 {
		return nil
		//return fmt.Errorf("delete of missing feedgen: %s %s", repo.Did, rkey)
	}

	if err := b.db.Exec("DELETE FROM feed_generators WHERE id = ?", feedgen.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteThreadgate(ctx context.Context, repo *Repo, rkey string) error {
	var threadgate ThreadGate
	if err := b.db.Find(&threadgate, "author = ? AND rkey = ?", repo.ID, rkey).Error; err != nil {
		return err
	}

	if threadgate.ID == 0 {
		return nil
		//return fmt.Errorf("delete of missing threadgate: %s %s", repo.Did, rkey)
	}

	if err := b.db.Exec("DELETE FROM thread_gates WHERE id = ?", threadgate.ID).Error; err != nil {
		return err
	}

	return nil
}

func (b *PostgresBackend) HandleDeleteProfile(ctx context.Context, repo *Repo, rkey string) error {
	var profile Profile
	if err := b.db.Find(&profile, "repo = ?", repo.ID).Error; err != nil {
		return err
	}

	if profile.ID == 0 {
		return nil
	}

	if err := b.db.Exec("DELETE FROM profiles WHERE id = ?", profile.ID).Error; err != nil {
		return err
	}

	return nil
}

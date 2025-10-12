package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/whyrusleeping/konbini/models"
	"github.com/whyrusleeping/market/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// PostgresBackend handles database operations
type PostgresBackend struct {
	db      *gorm.DB
	pgx     *pgxpool.Pool
	tracker RecordTracker

	client *xrpc.Client

	mydid  string
	myrepo *models.Repo

	relevantDids map[string]bool
	rdLk         sync.Mutex

	revCache *lru.TwoQueueCache[uint, string]

	repoCache *lru.TwoQueueCache[string, *Repo]
	reposLk   sync.Mutex

	postInfoCache *lru.TwoQueueCache[string, cachedPostInfo]
}

type cachedPostInfo struct {
	ID     uint
	Author uint
}

// NewPostgresBackend creates a new PostgresBackend
func NewPostgresBackend(mydid string, db *gorm.DB, pgx *pgxpool.Pool, client *xrpc.Client, tracker RecordTracker) (*PostgresBackend, error) {
	rc, _ := lru.New2Q[string, *Repo](1_000_000)
	pc, _ := lru.New2Q[string, cachedPostInfo](1_000_000)
	revc, _ := lru.New2Q[uint, string](1_000_000)

	b := &PostgresBackend{
		client:        client,
		mydid:         mydid,
		db:            db,
		pgx:           pgx,
		tracker:       tracker,
		relevantDids:  make(map[string]bool),
		repoCache:     rc,
		postInfoCache: pc,
		revCache:      revc,
	}

	r, err := b.GetOrCreateRepo(context.TODO(), mydid)
	if err != nil {
		return nil, err
	}

	b.myrepo = r
	return b, nil
}

// TrackMissingRecord implements the RecordTracker interface
func (b *PostgresBackend) TrackMissingRecord(identifier string, wait bool) {
	if b.tracker != nil {
		b.tracker.TrackMissingRecord(identifier, wait)
	}
}

// DidToID converts a DID to a database ID
func (b *PostgresBackend) DidToID(ctx context.Context, did string) (uint, error) {
	r, err := b.GetOrCreateRepo(ctx, did)
	if err != nil {
		return 0, err
	}
	return r.ID, nil
}

func (b *PostgresBackend) GetOrCreateRepo(ctx context.Context, did string) (*Repo, error) {
	r, ok := b.repoCache.Get(did)
	if !ok {
		b.reposLk.Lock()

		r, ok = b.repoCache.Get(did)
		if !ok {
			r = &Repo{}
			r.Did = did
			b.repoCache.Add(did, r)
		}

		b.reposLk.Unlock()
	}

	r.Lk.Lock()
	defer r.Lk.Unlock()
	if r.Setup {
		return r, nil
	}

	row := b.pgx.QueryRow(ctx, "SELECT id, created_at, did FROM repos WHERE did = $1", did)

	err := row.Scan(&r.ID, &r.CreatedAt, &r.Did)
	if err == nil {
		// found it!
		r.Setup = true
		return r, nil
	}

	if err != pgx.ErrNoRows {
		return nil, err
	}

	r.Did = did
	if err := b.db.Create(r).Error; err != nil {
		return nil, err
	}

	r.Setup = true

	return r, nil
}

func (b *PostgresBackend) GetOrCreateList(ctx context.Context, uri string) (*List, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := b.GetOrCreateRepo(ctx, puri.Did)
	if err != nil {
		return nil, err
	}

	// TODO: needs upsert treatment when we actually find the list
	var list List
	if err := b.db.FirstOrCreate(&list, map[string]any{
		"author": r.ID,
		"rkey":   puri.Rkey,
	}).Error; err != nil {
		return nil, err
	}
	return &list, nil
}

func (b *PostgresBackend) postIDForUri(ctx context.Context, uri string) (uint, error) {
	// getPostByUri implicitly fills the cache
	p, err := b.postInfoForUri(ctx, uri)
	if err != nil {
		return 0, err
	}

	return p.ID, nil
}

func (b *PostgresBackend) postInfoForUri(ctx context.Context, uri string) (cachedPostInfo, error) {
	v, ok := b.postInfoCache.Get(uri)
	if ok {
		return v, nil
	}

	// getPostByUri implicitly fills the cache
	p, err := b.getOrCreatePostBare(ctx, uri)
	if err != nil {
		return cachedPostInfo{}, err
	}

	return cachedPostInfo{ID: p.ID, Author: p.Author}, nil
}

func (b *PostgresBackend) tryLoadPostInfo(ctx context.Context, uid uint, rkey string) (*Post, error) {
	var p Post
	q := "SELECT id, author FROM posts WHERE author = $1 AND rkey = $2"
	if err := b.pgx.QueryRow(ctx, q, uid, rkey).Scan(&p.ID, &p.Author); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &p, nil
}

func (b *PostgresBackend) getOrCreatePostBare(ctx context.Context, uri string) (*Post, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := b.GetOrCreateRepo(ctx, puri.Did)
	if err != nil {
		return nil, err
	}

	post, err := b.tryLoadPostInfo(ctx, r.ID, puri.Rkey)
	if err != nil {
		return nil, err
	}

	if post == nil {
		post = &Post{
			Rkey:     puri.Rkey,
			Author:   r.ID,
			NotFound: true,
		}

		err := b.pgx.QueryRow(ctx, "INSERT INTO posts (rkey, author, not_found) VALUES ($1, $2, $3) RETURNING id", puri.Rkey, r.ID, true).Scan(&post.ID)
		if err != nil {
			pgErr, ok := err.(*pgconn.PgError)
			if !ok || pgErr.Code != "23505" {
				return nil, err
			}

			out, err := b.tryLoadPostInfo(ctx, r.ID, puri.Rkey)
			if err != nil {
				return nil, fmt.Errorf("got duplicate post and still couldnt find it: %w", err)
			}
			if out == nil {
				return nil, fmt.Errorf("postgres is lying to us: %d %s", r.ID, puri.Rkey)
			}

			post = out
		}

	}

	b.postInfoCache.Add(uri, cachedPostInfo{
		ID:     post.ID,
		Author: post.Author,
	})

	return post, nil
}

func (b *PostgresBackend) GetPostByUri(ctx context.Context, uri string, fields string) (*Post, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := b.GetOrCreateRepo(ctx, puri.Did)
	if err != nil {
		return nil, err
	}

	q := "SELECT " + fields + " FROM posts WHERE author = ? AND rkey = ?"

	var post Post
	if err := b.db.Raw(q, r.ID, puri.Rkey).Scan(&post).Error; err != nil {
		return nil, err
	}

	if post.ID == 0 {
		post.Rkey = puri.Rkey
		post.Author = r.ID
		post.NotFound = true

		if err := b.db.Session(&gorm.Session{
			Logger: logger.Default.LogMode(logger.Silent),
		}).Create(&post).Error; err != nil {
			if !errors.Is(err, gorm.ErrDuplicatedKey) {
				return nil, err
			}
			if err := b.db.Find(&post, "author = ? AND rkey = ?", r.ID, puri.Rkey).Error; err != nil {
				return nil, fmt.Errorf("got duplicate post and still couldnt find it: %w", err)
			}
		}

	}

	b.postInfoCache.Add(uri, cachedPostInfo{
		ID:     post.ID,
		Author: post.Author,
	})

	return &post, nil
}

func (b *PostgresBackend) revForRepo(rr *Repo) (string, error) {
	lrev, ok := b.revCache.Get(rr.ID)
	if ok {
		return lrev, nil
	}

	var rev string
	if err := b.pgx.QueryRow(context.TODO(), "SELECT COALESCE(rev, '') FROM sync_infos WHERE repo = $1", rr.ID).Scan(&rev); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}

	if rev != "" {
		b.revCache.Add(rr.ID, rev)
	}
	return rev, nil
}

func (b *PostgresBackend) AddRelevantDid(did string) {
	b.rdLk.Lock()
	defer b.rdLk.Unlock()
	b.relevantDids[did] = true
}

func (b *PostgresBackend) DidIsRelevant(did string) bool {
	b.rdLk.Lock()
	defer b.rdLk.Unlock()
	return b.relevantDids[did]
}

func (b *PostgresBackend) anyRelevantIdents(idents ...string) bool {
	for _, id := range idents {
		if strings.HasPrefix(id, "did:") {
			if b.DidIsRelevant(id) {
				return true
			}
		} else if strings.HasPrefix(id, "at://") {
			puri, err := syntax.ParseATURI(id)
			if err != nil {
				continue
			}

			if b.DidIsRelevant(puri.Authority().String()) {
				return true
			}
		}
	}

	return false
}

func (b *PostgresBackend) GetRelevantDids() []string {
	b.rdLk.Lock()
	var out []string
	for k := range b.relevantDids {
		out = append(out, k)
	}
	b.rdLk.Unlock()
	return out
}

func (b *PostgresBackend) GetRepoByID(ctx context.Context, id uint) (*models.Repo, error) {
	var r models.Repo
	if err := b.db.Find(&r, "id = ?", id).Error; err != nil {
		return nil, err
	}

	return &r, nil
}

func (b *PostgresBackend) checkPostExists(ctx context.Context, repo *Repo, rkey string) (bool, error) {
	var id uint
	var notfound bool
	if err := b.pgx.QueryRow(ctx, "SELECT id, not_found FROM posts WHERE author = $1 AND rkey = $2", repo.ID, rkey).Scan(&id, &notfound); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	if id != 0 && !notfound {
		return true, nil
	}

	return false, nil
}

func (b *PostgresBackend) LoadRelevantDids() error {
	ctx := context.TODO()

	if err := b.ensureFollowsScraped(ctx, b.mydid); err != nil {
		return fmt.Errorf("failed to scrape follows: %w", err)
	}

	r, err := b.GetOrCreateRepo(ctx, b.mydid)
	if err != nil {
		return err
	}

	var dids []string
	if err := b.db.Raw("select did from follows left join repos on follows.subject = repos.id where follows.author = ?", r.ID).Scan(&dids).Error; err != nil {
		return err
	}

	b.relevantDids[b.mydid] = true
	for _, d := range dids {
		fmt.Println("adding did: ", d)
		b.relevantDids[d] = true
	}

	return nil
}

type SyncInfo struct {
	Repo          uint `gorm:"index"`
	FollowsSynced bool
	Rev           string
}

func (b *PostgresBackend) ensureFollowsScraped(ctx context.Context, user string) error {
	r, err := b.GetOrCreateRepo(ctx, user)
	if err != nil {
		return err
	}

	var si SyncInfo
	if err := b.db.Find(&si, "repo = ?", r.ID).Error; err != nil {
		return err
	}

	// not found
	if si.Repo == 0 {
		if err := b.db.Create(&SyncInfo{
			Repo: r.ID,
		}).Error; err != nil {
			return err
		}
	}

	if si.FollowsSynced {
		return nil
	}

	var follows []Follow
	var cursor string
	for {
		resp, err := atproto.RepoListRecords(ctx, b.client, "app.bsky.graph.follow", cursor, 100, b.mydid, false)
		if err != nil {
			return err
		}

		for _, rec := range resp.Records {
			if fol, ok := rec.Value.Val.(*bsky.GraphFollow); ok {
				fr, err := b.GetOrCreateRepo(ctx, fol.Subject)
				if err != nil {
					return err
				}

				puri, err := syntax.ParseATURI(rec.Uri)
				if err != nil {
					return err
				}

				follows = append(follows, Follow{
					Created: time.Now(),
					Indexed: time.Now(),
					Rkey:    puri.RecordKey().String(),
					Author:  r.ID,
					Subject: fr.ID,
				})
			}
		}

		if resp.Cursor == nil || len(resp.Records) == 0 {
			break
		}
		cursor = *resp.Cursor
	}

	if err := b.db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(follows, 200).Error; err != nil {
		return err
	}

	if err := b.db.Model(SyncInfo{}).Where("repo = ?", r.ID).Update("follows_synced", true).Error; err != nil {
		return err
	}

	fmt.Println("Got follows: ", len(follows))

	return nil
}

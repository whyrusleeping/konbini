package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/parallel"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/util/cliutil"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/urfave/cli/v2"
	"github.com/whyrusleeping/market/models"
	. "github.com/whyrusleeping/market/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

var handleOpHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "handle_op_duration",
	Help:    "A histogram of op handling durations",
	Buckets: prometheus.ExponentialBuckets(1, 2, 15),
}, []string{"op", "collection"})

var doEmbedHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "do_embed_hist",
	Help:    "A histogram of embedding computation time",
	Buckets: prometheus.ExponentialBucketsRange(0.001, 30, 20),
}, []string{"model"})

var embeddingTimeHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "embed_timing",
	Help:    "A histogram of embedding computation time",
	Buckets: prometheus.ExponentialBucketsRange(0.001, 30, 20),
}, []string{"model", "phase", "host"})

var refreshEmbeddingHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "refresh_embed_timing",
	Help:    "A histogram of embedding refresh times",
	Buckets: prometheus.ExponentialBucketsRange(0.001, 30, 20),
}, []string{"host"})

var firehoseCursorGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "firehose_cursor",
}, []string{"stage"})

func main() {
	app := cli.App{
		Name: "konbini",
	}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "db-url",
			EnvVars: []string{"DATABASE_URL"},
		},
		&cli.StringFlag{
			Name: "handle",
		},
		&cli.IntFlag{
			Name:  "max-db-connections",
			Value: runtime.NumCPU(),
		},
	}
	app.Action = func(cctx *cli.Context) error {
		db, err := cliutil.SetupDatabase(cctx.String("db-url"), cctx.Int("max-db-connections"))
		if err != nil {
			return err
		}

		db.Logger = logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold:             500 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: false,
			Colorful:                  true,
		})

		//db.AutoMigrate(cursorRecord{})
		//db.AutoMigrate(MarketConfig{})
		db.AutoMigrate(Repo{})
		db.AutoMigrate(Post{})
		db.AutoMigrate(Follow{})
		db.AutoMigrate(Block{})
		db.AutoMigrate(Like{})
		db.AutoMigrate(Repost{})
		db.AutoMigrate(List{})
		db.AutoMigrate(ListItem{})
		db.AutoMigrate(ListBlock{})
		db.AutoMigrate(Profile{})
		db.AutoMigrate(ThreadGate{})
		db.AutoMigrate(FeedGenerator{})
		db.AutoMigrate(Image{})
		db.AutoMigrate(PostGate{})
		db.AutoMigrate(StarterPack{})
		db.AutoMigrate(SyncInfo{})

		ctx := context.TODO()

		rc, _ := lru.New2Q[string, *Repo](1_000_000)
		pc, _ := lru.New2Q[string, cachedPostInfo](1_000_000)
		revc, _ := lru.New2Q[uint, string](1_000_000)

		cfg, err := pgxpool.ParseConfig(cctx.String("db-url"))
		if err != nil {
			return err
		}

		if cfg.MaxConns < 8 {
			cfg.MaxConns = 8
		}

		pool, err := pgxpool.NewWithConfig(context.TODO(), cfg)
		if err != nil {
			return err
		}

		if err := pool.Ping(context.TODO()); err != nil {
			return err
		}

		handle := os.Getenv("BSKY_HANDLE")
		password := os.Getenv("BSKY_PASSWORD")

		dir := identity.DefaultDirectory()

		resp, err := dir.LookupHandle(ctx, syntax.Handle(handle))
		if err != nil {
			return err
		}
		mydid := resp.DID.String()

		cc := &xrpc.Client{
			Host: resp.PDSEndpoint(),
		}

		nsess, err := atproto.ServerCreateSession(ctx, cc, &atproto.ServerCreateSession_Input{
			Identifier: handle,
			Password:   password,
		})
		if err != nil {
			return err
		}

		cc.Auth = &xrpc.AuthInfo{
			AccessJwt:  nsess.AccessJwt,
			Did:        mydid,
			Handle:     nsess.Handle,
			RefreshJwt: nsess.RefreshJwt,
		}

		s := &Server{
			mydid:  mydid,
			client: cc,
			dir:    dir,
		}

		pgb := &PostgresBackend{
			relevantDids:  make(map[string]bool),
			s:             s,
			db:            db,
			postInfoCache: pc,
			repoCache:     rc,
			revCache:      revc,
			pgx:           pool,
		}
		s.backend = pgb

		if err := s.backend.loadRelevantDids(); err != nil {
			return fmt.Errorf("failed to load relevant dids set: %w", err)
		}

		go func() {
			if err := s.runApiServer(); err != nil {
				fmt.Println("failed to start api server: ", err)
			}
		}()

		seqno, err := loadLastSeq("sequence.txt")
		if err != nil {
			fmt.Println("failed to load sequence number, starting over", err)
		}

		return s.startLiveTail(ctx, seqno, 10, 20)
	}

	app.RunAndExitOnError()
}

type Server struct {
	backend *PostgresBackend

	dir identity.Directory

	client *xrpc.Client
	mydid  string

	seqLk   sync.Mutex
	lastSeq int64
}

func (s *Server) startLiveTail(ctx context.Context, curs int, parWorkers, maxQ int) error {
	slog.Info("starting live tail")

	// Connect to the Relay websocket
	urlStr := fmt.Sprintf("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", curs)

	d := websocket.DefaultDialer
	con, _, err := d.Dial(urlStr, http.Header{
		"User-Agent": []string{"market/0.0.1"},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to relay: %w", err)
	}

	var lelk sync.Mutex
	lastEvent := time.Now()

	go func() {
		for range time.Tick(time.Second) {
			lelk.Lock()
			let := lastEvent
			lelk.Unlock()

			if time.Since(let) > time.Second*30 {
				slog.Error("firehose connection timed out")
				con.Close()
				return
			}

		}

	}()

	var cclk sync.Mutex
	var completeCursor int64

	rsc := &stream.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			ctx := context.Background()

			firehoseCursorGauge.WithLabelValues("ingest").Set(float64(evt.Seq))

			s.seqLk.Lock()
			if evt.Seq > s.lastSeq {
				curs = int(evt.Seq)
				s.lastSeq = evt.Seq

				if evt.Seq%1000 == 0 {
					if err := storeLastSeq("sequence.txt", int(evt.Seq)); err != nil {
						fmt.Println("failed to store seqno: ", err)
					}
				}
			}
			s.seqLk.Unlock()

			lelk.Lock()
			lastEvent = time.Now()
			lelk.Unlock()

			if err := s.backend.HandleEvent(ctx, evt); err != nil {
				return fmt.Errorf("handle event (%s,%d): %w", evt.Repo, evt.Seq, err)
			}

			cclk.Lock()
			if evt.Seq > completeCursor {
				completeCursor = evt.Seq
				firehoseCursorGauge.WithLabelValues("complete").Set(float64(evt.Seq))
			}
			cclk.Unlock()

			return nil
		},
		RepoInfo: func(info *atproto.SyncSubscribeRepos_Info) error {
			return nil
		},
		// TODO: all the other event types
		Error: func(errf *stream.ErrorFrame) error {
			return fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
		},
	}

	sched := parallel.NewScheduler(parWorkers, maxQ, con.RemoteAddr().String(), rsc.EventHandler)

	//s.eventScheduler = sched
	//s.streamFinished = make(chan struct{})

	return stream.HandleRepoStream(ctx, con, sched, slog.Default())
}

func (s *Server) resolveAccountIdent(ctx context.Context, acc string) (string, error) {
	unesc, err := url.PathUnescape(acc)
	if err != nil {
		return "", err
	}

	acc = unesc
	if strings.HasPrefix(acc, "did:") {
		return acc, nil
	}

	resp, err := s.dir.LookupHandle(ctx, syntax.Handle(acc))
	if err != nil {
		return "", err
	}

	return resp.DID.String(), nil
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

func (b *PostgresBackend) getOrCreateRepo(ctx context.Context, did string) (*Repo, error) {
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

func (b *PostgresBackend) getOrCreateList(ctx context.Context, uri string) (*List, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := b.getOrCreateRepo(ctx, puri.Did)
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

type cachedPostInfo struct {
	ID     uint
	Author uint
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

	r, err := b.getOrCreateRepo(ctx, puri.Did)
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

func (b *PostgresBackend) getPostByUri(ctx context.Context, uri string, fields string) (*Post, error) {
	puri, err := util.ParseAtUri(uri)
	if err != nil {
		return nil, err
	}

	r, err := b.getOrCreateRepo(ctx, puri.Did)
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

func (b *PostgresBackend) ensureFollowsScraped(ctx context.Context, user string) error {
	r, err := b.getOrCreateRepo(ctx, user)
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
		resp, err := atproto.RepoListRecords(ctx, b.s.client, "app.bsky.graph.follow", cursor, 100, b.s.mydid, false)
		if err != nil {
			return err
		}

		for _, rec := range resp.Records {
			if fol, ok := rec.Value.Val.(*bsky.GraphFollow); ok {
				fr, err := b.getOrCreateRepo(ctx, fol.Subject)
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

func (b *PostgresBackend) loadRelevantDids() error {
	ctx := context.TODO()

	if err := b.ensureFollowsScraped(ctx, b.s.mydid); err != nil {
		return fmt.Errorf("failed to scrape follows: %w", err)
	}

	r, err := b.getOrCreateRepo(ctx, b.s.mydid)
	if err != nil {
		return err
	}

	var dids []string
	if err := b.db.Raw("select did from follows left join repos on follows.subject = repos.id where follows.author = ?", r.ID).Scan(&dids).Error; err != nil {
		return err
	}

	b.relevantDids[b.s.mydid] = true
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

func (b *PostgresBackend) didIsRelevant(did string) bool {
	b.rdLk.Lock()
	defer b.rdLk.Unlock()
	return b.relevantDids[did]
}

func (b *PostgresBackend) anyRelevantIdents(idents ...string) bool {
	for _, id := range idents {
		if strings.HasPrefix(id, "did:") {
			if b.didIsRelevant(id) {
				return true
			}
		} else if strings.HasPrefix(id, "at://") {
			puri, err := syntax.ParseATURI(id)
			if err != nil {
				continue
			}

			if b.didIsRelevant(puri.Authority().String()) {
				return true
			}
		}
	}

	return false
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

	pid, err := b.postIDForUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting like subject: %w", err)
	}

	if _, err := b.pgx.Exec(ctx, `INSERT INTO "likes" ("created","indexed","author","rkey","subject") VALUES ($1, $2, $3, $4, $5)`, created.Time(), time.Now(), repo.ID, rkey, pid); err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Code == "23505" {
			return nil
		}
		return err
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

	pid, err := b.postIDForUri(ctx, rec.Subject.Uri)
	if err != nil {
		return fmt.Errorf("getting repost subject: %w", err)
	}

	if _, err := b.pgx.Exec(ctx, `INSERT INTO "reposts" ("created","indexed","author","rkey","subject") VALUES ($1, $2, $3, $4, $5)`, created.Time(), time.Now(), repo.ID, rkey, pid); err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Code == "23505" {
			return nil
		}
		return err
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

func (b *PostgresBackend) getRepoByID(ctx context.Context, id uint) (*models.Repo, error) {
	var r models.Repo
	if err := b.db.Find(&r, "id = ?", id).Error; err != nil {
		return nil, err
	}

	return &r, nil
}

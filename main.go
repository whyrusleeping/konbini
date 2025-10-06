package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/parallel"
	"github.com/bluesky-social/indigo/util/cliutil"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gorilla/websocket"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/urfave/cli/v2"
	"gorm.io/gorm/logger"
)

var handleOpHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "handle_op_duration",
	Help:    "A histogram of op handling durations",
	Buckets: prometheus.ExponentialBuckets(1, 2, 15),
}, []string{"op", "collection"})

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
		db.AutoMigrate(Notification{})
		db.AutoMigrate(SequenceTracker{})

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

			missingProfiles: make(chan string, 1024),
			missingPosts:    make(chan string, 1024),
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

		myrepo, err := s.backend.getOrCreateRepo(ctx, mydid)
		if err != nil {
			return fmt.Errorf("failed to get repo record for our own did: %w", err)
		}
		s.myrepo = myrepo

		if err := s.backend.loadRelevantDids(); err != nil {
			return fmt.Errorf("failed to load relevant dids set: %w", err)
		}

		go func() {
			if err := s.runApiServer(); err != nil {
				fmt.Println("failed to start api server: ", err)
			}
		}()

		go func() {
			http.ListenAndServe(":4445", nil)
		}()

		go s.missingProfileFetcher()
		go s.missingPostFetcher()

		seqno, err := loadLastSeq(db, "firehose_seq")
		if err != nil {
			fmt.Println("failed to load sequence number, starting over", err)
		}

		return s.startLiveTail(ctx, int(seqno), 10, 20)
	}

	app.RunAndExitOnError()
}

type Server struct {
	backend *PostgresBackend

	dir identity.Directory

	client *xrpc.Client
	mydid  string
	myrepo *Repo

	seqLk   sync.Mutex
	lastSeq int64

	mpLk            sync.Mutex
	missingProfiles chan string
	missingPosts    chan string
}

func (s *Server) getXrpcClient() (*xrpc.Client, error) {
	// TODO: handle refreshing the token periodically
	return s.client, nil
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
					if err := storeLastSeq(s.backend.db, "firehose_seq", evt.Seq); err != nil {
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

const (
	NotifKindReply   = "reply"
	NotifKindLike    = "like"
	NotifKindMention = "mention"
	NotifKindRepost  = "repost"
)

func (s *Server) AddNotification(ctx context.Context, forUser, author uint, recordUri string, kind string) error {
	return s.backend.db.Create(&Notification{
		For:    forUser,
		Author: author,
		Source: recordUri,
		Kind:   kind,
	}).Error
}

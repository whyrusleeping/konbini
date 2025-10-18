package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/bluesky-social/indigo/atproto/identity/redisdir"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/util/cliutil"
	xrpclib "github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/urfave/cli/v2"
	"github.com/whyrusleeping/konbini/backend"
	"github.com/whyrusleeping/konbini/xrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	. "github.com/whyrusleeping/konbini/models"
)

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
		&cli.BoolFlag{
			Name: "jaeger",
		},
		&cli.StringFlag{
			Name: "handle",
		},
		&cli.IntFlag{
			Name:  "max-db-connections",
			Value: runtime.NumCPU(),
		},
		&cli.StringFlag{
			Name: "redis-url",
		},
		&cli.StringFlag{
			Name: "sync-config",
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

		if cctx.Bool("jaeger") {
			// Use Jaeger native exporter sending to port 14268
			jaegerUrl := "http://localhost:14268/api/traces"
			exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerUrl)))
			if err != nil {
				return err
			}

			env := os.Getenv("ENV")
			if env == "" {
				env = "development"
			}

			tp := tracesdk.NewTracerProvider(
				// Always be sure to batch in production.
				tracesdk.WithBatcher(exp),
				// Record information about this application in a Resource.
				tracesdk.WithResource(resource.NewWithAttributes(
					semconv.SchemaURL,
					semconv.ServiceNameKey.String("konbini"),
					attribute.String("env", env),         // DataDog
					attribute.String("environment", env), // Others
					attribute.Int64("ID", 1),
				)),
			)

			otel.SetTracerProvider(tp)
		}

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
		db.AutoMigrate(backend.SyncInfo{})
		db.AutoMigrate(Notification{})
		db.AutoMigrate(NotificationSeen{})
		db.AutoMigrate(SequenceTracker{})
		db.Exec("CREATE INDEX IF NOT EXISTS reposts_subject_idx ON reposts (subject)")
		db.Exec("CREATE INDEX IF NOT EXISTS posts_reply_to_idx ON posts (reply_to)")
		db.Exec("CREATE INDEX IF NOT EXISTS posts_in_thread_idx ON posts (in_thread)")

		ctx := context.TODO()

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

		if redisURL := cctx.String("redis-url"); redisURL != "" {
			rdir, err := redisdir.NewRedisDirectory(dir, redisURL, time.Minute, time.Second*10, time.Second*10, 100_000)
			if err != nil {
				return err
			}
			dir = rdir
		}

		resp, err := dir.LookupHandle(ctx, syntax.Handle(handle))
		if err != nil {
			return err
		}
		mydid := resp.DID.String()

		cc := &xrpclib.Client{
			Host: resp.PDSEndpoint(),
		}

		nsess, err := atproto.ServerCreateSession(ctx, cc, &atproto.ServerCreateSession_Input{
			Identifier: handle,
			Password:   password,
		})
		if err != nil {
			return err
		}

		cc.Auth = &xrpclib.AuthInfo{
			AccessJwt:  nsess.AccessJwt,
			Did:        mydid,
			Handle:     nsess.Handle,
			RefreshJwt: nsess.RefreshJwt,
		}

		s := &Server{
			mydid:  mydid,
			client: cc,
			dir:    dir,

			db: db,
		}

		pgb, err := backend.NewPostgresBackend(mydid, db, pool, cc, dir)
		if err != nil {
			return err
		}

		s.backend = pgb

		myrepo, err := s.backend.GetOrCreateRepo(ctx, mydid)
		if err != nil {
			return fmt.Errorf("failed to get repo record for our own did: %w", err)
		}
		s.myrepo = myrepo

		if err := s.backend.LoadRelevantDids(); err != nil {
			return fmt.Errorf("failed to load relevant dids set: %w", err)
		}

		// Start custom API server (for the custom frontend)
		go func() {
			if err := s.runApiServer(); err != nil {
				fmt.Println("failed to start api server: ", err)
			}
		}()

		// Start XRPC server (for official Bluesky app compatibility)
		go func() {
			xrpcServer := xrpc.NewServer(db, dir, pgb)
			if err := xrpcServer.Start(":4446"); err != nil {
				fmt.Println("failed to start XRPC server: ", err)
			}
		}()

		// Start pprof server
		go func() {
			http.ListenAndServe(":4445", nil)
		}()

		sc := SyncConfig{
			Backends: []SyncBackend{
				{
					Type: "firehose",
					Host: "bsky.network",
				},
			},
		}

		if scfn := cctx.String("sync-config"); scfn != "" {
			{
				scfi, err := os.Open(scfn)
				if err != nil {
					return err
				}
				defer scfi.Close()

				var lsc SyncConfig
				if err := json.NewDecoder(scfi).Decode(&lsc); err != nil {
					return err
				}
				sc = lsc
			}
		}

		/*
			sc.Backends[0] = SyncBackend{
				Type: "jetstream",
				Host: "jetstream1.us-west.bsky.network",
			}
		*/

		return s.StartSyncEngine(ctx, &sc)

	}

	app.RunAndExitOnError()
}

type Server struct {
	backend *backend.PostgresBackend

	dir identity.Directory

	client *xrpclib.Client
	mydid  string
	myrepo *Repo

	seqLk   sync.Mutex
	lastSeq int64

	mpLk sync.Mutex

	db *gorm.DB
}

func (s *Server) getXrpcClient() (*xrpclib.Client, error) {
	// TODO: handle refreshing the token periodically
	return s.client, nil
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

func (s *Server) rescanRepo(ctx context.Context, did string) error {
	resp, err := s.dir.LookupDID(ctx, syntax.DID(did))
	if err != nil {
		return err
	}

	s.backend.AddRelevantDid(did)

	c := &xrpclib.Client{
		Host: resp.PDSEndpoint(),
	}

	repob, err := atproto.SyncGetRepo(ctx, c, did, "")
	if err != nil {
		return err
	}

	rep, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repob))
	if err != nil {
		return err
	}

	return rep.ForEach(ctx, "", func(k string, v cid.Cid) error {
		blk, err := rep.Blockstore().Get(ctx, v)
		if err != nil {
			slog.Error("record missing in repo", "path", k, "cid", v, "error", err)
			return nil
		}

		d := blk.RawData()
		if err := s.backend.HandleCreate(ctx, did, "", k, &d, &v); err != nil {
			slog.Error("failed to index record", "path", k, "cid", v, "error", err)
		}
		return nil
	})

}

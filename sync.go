package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/cmd/relay/stream"
	"github.com/bluesky-social/indigo/cmd/relay/stream/schedulers/parallel"
	jsclient "github.com/bluesky-social/jetstream/pkg/client"
	jsparallel "github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/gorilla/websocket"
)

type SyncConfig struct {
	Backends []SyncBackend `json:"backends"`
}

type SyncBackend struct {
	Type       string `json:"type"`
	Host       string `json:"host"`
	MaxWorkers int    `json:"max_workers,omitempty"`
}

func (s *Server) StartSyncEngine(ctx context.Context, sc *SyncConfig) error {
	for _, be := range sc.Backends {
		switch be.Type {
		case "firehose":
			go s.runSyncFirehose(ctx, be)
		case "jetstream":
			go s.runSyncJetstream(ctx, be)
		default:
			return fmt.Errorf("unrecognized sync backend type: %q", be.Type)
		}
	}

	<-ctx.Done()
	return fmt.Errorf("exiting sync routine")
}

const failureTimeInterval = time.Second * 5

func (s *Server) runSyncFirehose(ctx context.Context, be SyncBackend) {
	var failures int
	for {
		seqno, err := loadLastSeq(s.db, be.Host)
		if err != nil {
			fmt.Println("failed to load sequence number, starting over", err)
		}

		maxWorkers := 10
		if be.MaxWorkers != 0 {
			maxWorkers = be.MaxWorkers
		}

		start := time.Now()
		if err := s.startLiveTail(ctx, be.Host, int(seqno), maxWorkers, 20); err != nil {
			slog.Error("firehose connection lost", "host", be.Host, "error", err)
		}

		elapsed := time.Since(start)

		if elapsed > failureTimeInterval {
			failures = 0
			continue
		}
		failures++

		delay := delayForFailureCount(failures)
		slog.Warn("retrying connection after delay", "host", be.Host, "delay", delay)
	}
}

func (s *Server) runSyncJetstream(ctx context.Context, be SyncBackend) {
	var failures int
	for {
		// Load last cursor (stored as sequence number in same table)
		cursor, err := loadLastSeq(s.db, be.Host)
		if err != nil {
			slog.Warn("failed to load jetstream cursor, starting from live", "error", err)
			cursor = 0
		}

		maxWorkers := 10
		if be.MaxWorkers != 0 {
			maxWorkers = be.MaxWorkers
		}

		start := time.Now()
		if err := s.startJetstreamTail(ctx, be.Host, cursor, maxWorkers); err != nil {
			slog.Error("jetstream connection lost", "host", be.Host, "error", err)
		}

		elapsed := time.Since(start)

		if elapsed > failureTimeInterval {
			failures = 0
			continue
		}
		failures++

		delay := delayForFailureCount(failures)
		slog.Warn("retrying jetstream connection after delay", "host", be.Host, "delay", delay)
		time.Sleep(delay)
	}
}

func delayForFailureCount(n int) time.Duration {
	if n < 5 {
		return (time.Second * 5) + (time.Second * 2 * time.Duration(n))
	}

	return time.Second * 30
}

func (s *Server) startLiveTail(ctx context.Context, host string, curs int, parWorkers, maxQ int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Info("starting live tail")

	// Connect to the Relay websocket
	urlStr := fmt.Sprintf("wss://%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", host, curs)

	d := websocket.DefaultDialer
	con, _, err := d.Dial(urlStr, http.Header{
		"User-Agent": []string{"konbini/0.0.1"},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to relay: %w", err)
	}

	var lelk sync.Mutex
	lastEvent := time.Now()

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				lelk.Lock()
				let := lastEvent
				lelk.Unlock()

				if time.Since(let) > time.Second*30 {
					slog.Error("firehose connection timed out")
					con.Close()
					return
				}
			case <-ctx.Done():
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
					if err := storeLastSeq(s.db, host, evt.Seq); err != nil {
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

	return stream.HandleRepoStream(ctx, con, sched, slog.Default())
}

func (s *Server) startJetstreamTail(ctx context.Context, host string, cursor int64, parWorkers int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Info("starting jetstream tail", "host", host, "cursor", cursor)

	// Create a scheduler for parallel processing
	lastStored := int64(0)
	sched := jsparallel.NewScheduler(
		parWorkers,
		host,
		slog.Default(),
		func(ctx context.Context, event *models.Event) error {
			// Update cursor tracking
			s.seqLk.Lock()
			if event.TimeUS > s.lastSeq {
				s.lastSeq = event.TimeUS
				if event.TimeUS-lastStored > 1_000_000 {
					// Store checkpoint periodically
					if err := storeLastSeq(s.db, host, event.TimeUS); err != nil {
						slog.Error("failed to store jetstream cursor", "error", err)
					}
					lastStored = event.TimeUS
				}
			}
			s.seqLk.Unlock()

			// Update metrics
			firehoseCursorGauge.WithLabelValues("ingest").Set(float64(event.TimeUS))

			// Convert Jetstream event to ATProto event format
			if event.Commit != nil {

				if err := s.backend.HandleEventJetstream(ctx, event); err != nil {
					return fmt.Errorf("handle event (%s,%d): %w", event.Did, event.TimeUS, err)
				}

				firehoseCursorGauge.WithLabelValues("complete").Set(float64(event.TimeUS))
			}

			return nil
		},
	)

	// Configure Jetstream client
	config := jsclient.DefaultClientConfig()
	config.WebsocketURL = fmt.Sprintf("wss://%s/subscribe", host)

	// Prepare cursor pointer
	var cursorPtr *int64
	if cursor > 0 {
		cursorPtr = &cursor
	}

	// Create and connect client
	client, err := jsclient.NewClient(
		config,
		slog.Default(),
		sched,
	)
	if err != nil {
		return fmt.Errorf("create jetstream client: %w", err)
	}

	// Start reading from Jetstream
	return client.ConnectAndRead(ctx, cursorPtr)
}

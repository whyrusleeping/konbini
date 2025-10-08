package xrpc

import (
	"log/slog"
	"net/http"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/xrpc/actor"
	"github.com/whyrusleeping/konbini/xrpc/feed"
	"github.com/whyrusleeping/konbini/xrpc/graph"
	"github.com/whyrusleeping/konbini/xrpc/labeler"
	"github.com/whyrusleeping/konbini/xrpc/notification"
	"github.com/whyrusleeping/konbini/xrpc/repo"
	"github.com/whyrusleeping/konbini/xrpc/unspecced"
	"gorm.io/gorm"
)

// Server represents the XRPC API server
type Server struct {
	e        *echo.Echo
	db       *gorm.DB
	dir      identity.Directory
	backend  Backend
	hydrator *hydration.Hydrator
}

// Backend interface for data access
type Backend interface {
	// Add methods as needed for data access

	TrackMissingActor(did string)
	TrackMissingFeedGenerator(uri string)
}

// NewServer creates a new XRPC server
func NewServer(db *gorm.DB, dir identity.Directory, backend Backend) *Server {
	e := echo.New()
	e.HidePort = true
	e.HideBanner = true

	// CORS middleware
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodOptions},
		AllowHeaders: []string{"*"},
	}))

	// Logging middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	s := &Server{
		e:        e,
		db:       db,
		dir:      dir,
		backend:  backend,
		hydrator: hydration.NewHydrator(db, dir),
	}

	s.hydrator.SetMissingActorCallback(backend.TrackMissingActor)
	s.hydrator.SetMissingFeedGeneratorCallback(backend.TrackMissingFeedGenerator)

	// Register XRPC endpoints
	s.registerEndpoints()

	return s
}

// Start starts the XRPC server
func (s *Server) Start(addr string) error {
	slog.Info("starting XRPC server", "addr", addr)
	return s.e.Start(addr)
}

// registerEndpoints registers all XRPC endpoints
func (s *Server) registerEndpoints() {
	// XRPC endpoints follow the pattern: /xrpc/<namespace>.<method>
	xrpcGroup := s.e.Group("/xrpc")

	// com.atproto.identity.*
	xrpcGroup.GET("/com.atproto.identity.resolveHandle", s.handleResolveHandle)

	// com.atproto.repo.*
	xrpcGroup.GET("/com.atproto.repo.getRecord", func(c echo.Context) error {
		return repo.HandleGetRecord(c, s.db, s.hydrator)
	})

	// app.bsky.actor.*
	xrpcGroup.GET("/app.bsky.actor.getProfile", func(c echo.Context) error {
		return actor.HandleGetProfile(c, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.actor.getProfiles", func(c echo.Context) error {
		return actor.HandleGetProfiles(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.actor.getPreferences", func(c echo.Context) error {
		return actor.HandleGetPreferences(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.POST("/app.bsky.actor.putPreferences", func(c echo.Context) error {
		return actor.HandlePutPreferences(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.GET("/app.bsky.actor.searchActors", s.handleSearchActors)
	xrpcGroup.GET("/app.bsky.actor.searchActorsTypeahead", s.handleSearchActorsTypeahead)

	// app.bsky.feed.*
	xrpcGroup.GET("/app.bsky.feed.getTimeline", func(c echo.Context) error {
		return feed.HandleGetTimeline(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.GET("/app.bsky.feed.getAuthorFeed", func(c echo.Context) error {
		return feed.HandleGetAuthorFeed(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.feed.getPostThread", func(c echo.Context) error {
		return feed.HandleGetPostThread(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.feed.getPosts", func(c echo.Context) error {
		return feed.HandleGetPosts(c, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.feed.getLikes", func(c echo.Context) error {
		return feed.HandleGetLikes(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.feed.getRepostedBy", func(c echo.Context) error {
		return feed.HandleGetRepostedBy(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.feed.getActorLikes", func(c echo.Context) error {
		return feed.HandleGetActorLikes(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.GET("/app.bsky.feed.getFeed", func(c echo.Context) error {
		return feed.HandleGetFeed(c, s.db, s.hydrator, s.dir)
	})
	xrpcGroup.GET("/app.bsky.feed.getFeedGenerator", func(c echo.Context) error {
		return feed.HandleGetFeedGenerator(c, s.db, s.hydrator, s.dir)
	})

	// app.bsky.graph.*
	xrpcGroup.GET("/app.bsky.graph.getFollows", func(c echo.Context) error {
		return graph.HandleGetFollows(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.graph.getFollowers", func(c echo.Context) error {
		return graph.HandleGetFollowers(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.graph.getBlocks", func(c echo.Context) error {
		return graph.HandleGetBlocks(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.GET("/app.bsky.graph.getMutes", func(c echo.Context) error {
		return graph.HandleGetMutes(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.GET("/app.bsky.graph.getRelationships", func(c echo.Context) error {
		return graph.HandleGetRelationships(c, s.db, s.hydrator)
	})
	xrpcGroup.GET("/app.bsky.graph.getLists", s.handleGetLists)
	xrpcGroup.GET("/app.bsky.graph.getList", s.handleGetList)

	// app.bsky.notification.*
	xrpcGroup.GET("/app.bsky.notification.listNotifications", func(c echo.Context) error {
		return notification.HandleListNotifications(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.GET("/app.bsky.notification.getUnreadCount", func(c echo.Context) error {
		return notification.HandleGetUnreadCount(c, s.db, s.hydrator)
	}, s.requireAuth)
	xrpcGroup.POST("/app.bsky.notification.updateSeen", func(c echo.Context) error {
		return notification.HandleUpdateSeen(c, s.db, s.hydrator)
	}, s.requireAuth)

	// app.bsky.labeler.*
	xrpcGroup.GET("/app.bsky.labeler.getServices", func(c echo.Context) error {
		return labeler.HandleGetServices(c)
	})

	// app.bsky.unspecced.*
	xrpcGroup.GET("/app.bsky.unspecced.getConfig", func(c echo.Context) error {
		return unspecced.HandleGetConfig(c)
	})
	xrpcGroup.GET("/app.bsky.unspecced.getTrendingTopics", func(c echo.Context) error {
		return unspecced.HandleGetTrendingTopics(c)
	})
	xrpcGroup.GET("/app.bsky.unspecced.getPostThreadV2", func(c echo.Context) error {
		return unspecced.HandleGetPostThreadV2(c, s.db, s.hydrator)
	})
}

// XRPCError creates a properly formatted XRPC error response
func XRPCError(c echo.Context, statusCode int, errType, message string) error {
	return c.JSON(statusCode, map[string]interface{}{
		"error":   errType,
		"message": message,
	})
}

// getUserDID extracts the viewer DID from the request context
// Returns empty string if not authenticated
func getUserDID(c echo.Context) string {
	did := c.Get("viewer")
	if did == nil {
		return ""
	}
	if s, ok := did.(string); ok {
		return s
	}
	return ""
}

func (s *Server) handleSearchActors(c echo.Context) error {
	return XRPCError(c, http.StatusNotImplemented, "NotImplemented", "Not yet implemented")
}

func (s *Server) handleSearchActorsTypeahead(c echo.Context) error {
	return XRPCError(c, http.StatusNotImplemented, "NotImplemented", "Not yet implemented")
}

func (s *Server) handleGetLists(c echo.Context) error {
	return XRPCError(c, http.StatusNotImplemented, "NotImplemented", "Not yet implemented")
}

func (s *Server) handleGetList(c echo.Context) error {
	return XRPCError(c, http.StatusNotImplemented, "NotImplemented", "Not yet implemented")
}

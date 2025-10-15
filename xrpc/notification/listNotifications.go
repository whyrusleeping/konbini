package notification

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	models "github.com/whyrusleeping/konbini/models"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// HandleListNotifications implements app.bsky.notification.listNotifications
func HandleListNotifications(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]any{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	// Parse limit
	limit := 50
	if limitParam := c.QueryParam("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	// Parse cursor (notification ID)
	var cursor uint
	if cursorParam := c.QueryParam("cursor"); cursorParam != "" {
		if c, err := strconv.ParseUint(cursorParam, 10, 64); err == nil {
			cursor = uint(c)
		}
	}

	ctx := c.Request().Context()

	// Query notifications for viewer with CIDs from source records
	type notifRow struct {
		ID        uint
		Kind      string
		AuthorDid string
		Source    string
		SourceCid string
		CreatedAt string
	}
	var rows []notifRow

	// This query tries to fetch the CID from the source record
	// depending on the notification kind (like, repost, reply, etc.)
	query := `
		SELECT
			n.id,
			n.kind,
			r.did as author_did,
			n.source,
			n.source_cid,
			n.created_at
		FROM notifications n
		JOIN repos r ON r.id = n.author
		LEFT JOIN repos r2 ON r2.id = n.author
		WHERE n.for = (SELECT id FROM repos WHERE did = ?)
	`
	if cursor > 0 {
		query += ` AND n.id < ?`
	}
	query += ` ORDER BY n.created_at DESC LIMIT ?`

	var queryArgs []any
	queryArgs = append(queryArgs, viewer)
	if cursor > 0 {
		queryArgs = append(queryArgs, cursor)
	}
	queryArgs = append(queryArgs, limit)

	if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to query notifications",
		})
	}

	// Hydrate notifications
	notifications := make([]*bsky.NotificationListNotifications_Notification, 0)
	for _, row := range rows {
		authorInfo, err := hydrator.HydrateActor(ctx, row.AuthorDid)
		if err != nil {
			continue
		}

		// Skip notifications without CIDs as they're invalid
		if row.SourceCid == "" {
			continue
		}

		// Fetch and decode the raw record
		recordDecoder, err := fetchNotificationRecord(db, row.Source, row.Kind)
		if err != nil {
			continue
		}

		notif := &bsky.NotificationListNotifications_Notification{
			Uri:       row.Source,
			Cid:       row.SourceCid,
			Author:    views.ProfileView(authorInfo),
			Reason:    mapNotifKind(row.Kind),
			Record:    recordDecoder,
			IsRead:    false,
			IndexedAt: row.CreatedAt,
		}

		notifications = append(notifications, notif)
	}

	// Generate next cursor
	var cursorPtr *string
	if len(rows) > 0 {
		cursor := strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
		cursorPtr = &cursor
	}

	var lastSeen time.Time
	if err := db.Raw("SELECT seen_at FROM notification_seens WHERE repo = (select id from repos where did = ?)", viewer).Scan(&lastSeen).Error; err != nil {
		return err
	}

	var lastSeenStr *string
	if !lastSeen.IsZero() {
		s := lastSeen.Format(time.RFC3339)
		lastSeenStr = &s
	}

	output := &bsky.NotificationListNotifications_Output{
		Notifications: notifications,
		Cursor:        cursorPtr,
		SeenAt:        lastSeenStr,
	}

	return c.JSON(http.StatusOK, output)
}

// HandleGetUnreadCount implements app.bsky.notification.getUnreadCount
func HandleGetUnreadCount(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]any{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	var repo models.Repo
	if err := db.Find(&repo, "did = ?", viewer).Error; err != nil {
		return err
	}

	var lastSeen time.Time
	if err := db.Raw("SELECT seen_at FROM notification_seens WHERE repo = ?", repo.ID).Scan(&lastSeen).Error; err != nil {
		return err
	}

	var count int
	query := `SELECT count(*) FROM notifications WHERE created_at > ? AND for = ?`
	if err := db.Raw(query, lastSeen, repo.ID).Scan(&count).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to count unread notifications",
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"count": count,
	})
}

// HandleUpdateSeen implements app.bsky.notification.updateSeen
func HandleUpdateSeen(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]any{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	var body bsky.NotificationUpdateSeen_Input
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":   "InvalidRequest",
			"message": "invalid request body",
		})
	}

	// Parse the seenAt timestamp
	seenAt, err := time.Parse(time.RFC3339, body.SeenAt)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]any{
			"error":   "InvalidRequest",
			"message": "invalid seenAt timestamp",
		})
	}

	// Get the viewer's repo ID
	var repoID uint
	if err := db.Raw("SELECT id FROM repos WHERE did = ?", viewer).Scan(&repoID).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to find viewer repo",
		})
	}

	if repoID == 0 {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "viewer repo not found",
		})
	}

	// Upsert the NotificationSeen record
	notifSeen := models.NotificationSeen{
		Repo:   repoID,
		SeenAt: seenAt,
	}

	err = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "repo"}},
		DoUpdates: clause.AssignmentColumns([]string{"seen_at"}),
	}).Create(&notifSeen).Error

	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{
			"error":   "InternalError",
			"message": "failed to update seen timestamp",
		})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

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

func mapNotifKind(kind string) string {
	switch kind {
	case "reply":
		return "reply"
	case "like":
		return "like"
	case "repost":
		return "repost"
	case "mention":
		return "mention"
	case "follow":
		return "follow"
	default:
		return kind
	}
}

// fetchNotificationRecord fetches and decodes the raw record for a notification
func fetchNotificationRecord(db *gorm.DB, sourceURI string, kind string) (*util.LexiconTypeDecoder, error) {
	// Parse the source URI to extract DID and rkey
	// URI format: at://did:plc:xxx/collection/rkey
	did := extractDIDFromURI(sourceURI)
	rkey := extractRkeyFromURI(sourceURI)

	if did == "" || rkey == "" {
		return nil, fmt.Errorf("invalid source URI")
	}

	var raw []byte
	var err error

	// Fetch raw data based on notification kind
	switch kind {
	case "reply", "mention", "quote":
		// These reference posts
		err = db.Raw(`
			SELECT p.raw
			FROM posts p
			JOIN repos r ON r.id = p.author
			WHERE r.did = ? AND p.rkey = ?
		`, did, rkey).Scan(&raw).Error

	case "like":
		// we don't store the raw like objects, so we just reconstruct it here...
		// These reference like records
		var like models.Like
		err = db.Raw(`
			SELECT *
			FROM likes l
			JOIN repos r ON r.id = l.author
			WHERE r.did = ? AND l.rkey = ?
		`, did, rkey).Scan(&like).Error

		lk := bsky.FeedLike{
			CreatedAt: like.Created.Format(time.RFC3339),
			Subject: &atproto.RepoStrongRef{
				Cid: "",
				Uri: "",
			},
		}
		buf := new(bytes.Buffer)
		if err := lk.MarshalCBOR(buf); err != nil {
			return nil, fmt.Errorf("failed to marshal reconstructed like: %w", err)
		}
		raw = buf.Bytes()

	case "repost":
		// These reference repost records
		err = db.Raw(`
			SELECT r.raw
			FROM reposts r
			JOIN repos repo ON repo.id = r.author
			WHERE repo.did = ? AND r.rkey = ?
		`, did, rkey).Scan(&raw).Error

	case "follow":
		// These reference follow records
		err = db.Raw(`
			SELECT f.raw
			FROM follows f
			JOIN repos r ON r.id = f.author
			WHERE r.did = ? AND f.rkey = ?
		`, did, rkey).Scan(&raw).Error

	default:
		return nil, fmt.Errorf("unknown notification kind: %s", kind)
	}

	if err != nil || len(raw) == 0 {
		return nil, fmt.Errorf("failed to fetch record: %w", err)
	}

	// Decode the CBOR data
	decoded, err := lexutil.CborDecodeValue(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to decode CBOR: %w", err)
	}

	return &util.LexiconTypeDecoder{
		Val: decoded,
	}, nil
}

func extractDIDFromURI(uri string) string {
	// URI format: at://did:plc:xxx/collection/rkey
	if len(uri) < 5 || uri[:5] != "at://" {
		return ""
	}
	parts := []rune(uri[5:])
	for i, r := range parts {
		if r == '/' {
			return string(parts[:i])
		}
	}
	return string(parts)
}

func extractRkeyFromURI(uri string) string {
	// URI format: at://did:plc:xxx/collection/rkey
	if len(uri) < 5 || uri[:5] != "at://" {
		return ""
	}
	// Find last slash
	for i := len(uri) - 1; i >= 5; i-- {
		if uri[i] == '/' {
			return uri[i+1:]
		}
	}
	return ""
}

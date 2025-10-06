package notification

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/whyrusleeping/konbini/hydration"
	"github.com/whyrusleeping/konbini/views"
	"gorm.io/gorm"
)

// HandleListNotifications implements app.bsky.notification.listNotifications
func HandleListNotifications(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
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

	var queryArgs []interface{}
	queryArgs = append(queryArgs, viewer)
	if cursor > 0 {
		queryArgs = append(queryArgs, cursor)
	}
	queryArgs = append(queryArgs, limit)

	if err := db.Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":   "InternalError",
			"message": "failed to query notifications",
		})
	}

	// Hydrate notifications
	notifications := make([]interface{}, 0)
	for _, row := range rows {
		authorInfo, err := hydrator.HydrateActor(ctx, row.AuthorDid)
		if err != nil {
			continue
		}

		notif := map[string]interface{}{
			"uri":       row.Source,
			"author":    views.ProfileView(authorInfo),
			"reason":    mapNotifKind(row.Kind),
			"record":    nil, // Could hydrate the source record here
			"isRead":    false,
			"indexedAt": row.CreatedAt,
			"labels":    []interface{}{},
		}

		// Only include CID if we have one (required field)
		if row.SourceCid != "" {
			notif["cid"] = row.SourceCid
		} else {
			// Skip notifications without CIDs as they're invalid
			continue
		}

		notifications = append(notifications, notif)
	}

	// Generate next cursor
	var nextCursor string
	if len(rows) > 0 {
		nextCursor = strconv.FormatUint(uint64(rows[len(rows)-1].ID), 10)
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"notifications": notifications,
		"cursor":        nextCursor,
	})
}

// HandleGetUnreadCount implements app.bsky.notification.getUnreadCount
func HandleGetUnreadCount(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	// For now, return 0 - we'd need to track read state in the database
	return c.JSON(http.StatusOK, map[string]interface{}{
		"count": 0,
	})
}

// HandleUpdateSeen implements app.bsky.notification.updateSeen
func HandleUpdateSeen(c echo.Context, db *gorm.DB, hydrator *hydration.Hydrator) error {
	viewer := getUserDID(c)
	if viewer == "" {
		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"error":   "AuthenticationRequired",
			"message": "authentication required",
		})
	}

	// For now, just return success - we'd need to track seen timestamps in the database
	return c.JSON(http.StatusOK, map[string]interface{}{})
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
	default:
		return kind
	}
}

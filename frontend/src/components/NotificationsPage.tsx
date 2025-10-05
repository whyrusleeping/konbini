import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Notification } from '../types';
import { ApiClient } from '../api';
import { formatRelativeTime, getBlobUrl, getPostUrl } from '../utils';
import { Link } from 'react-router-dom';
import './NotificationsPage.css';

export const NotificationsPage: React.FC = () => {
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);
  const observerTarget = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fetchNotifications = async () => {
      try {
        setLoading(true);
        const data = await ApiClient.getNotifications();
        setNotifications(data.notifications);
        setCursor(data.cursor || null);
        setHasMore(!!(data.cursor && data.notifications.length > 0));
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load notifications');
      } finally {
        setLoading(false);
      }
    };

    fetchNotifications();
  }, []);

  const fetchMoreNotifications = useCallback(async (cursorToUse: string) => {
    if (loadingMore || !hasMore) return;

    try {
      setLoadingMore(true);
      const data = await ApiClient.getNotifications(cursorToUse);
      setNotifications(prev => [...prev, ...data.notifications]);
      setCursor(data.cursor || null);
      setHasMore(!!(data.cursor && data.notifications.length > 0));
    } catch (err) {
      console.error('Failed to fetch more notifications:', err);
    } finally {
      setLoadingMore(false);
    }
  }, [loadingMore, hasMore]);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loadingMore && !loading && cursor) {
          fetchMoreNotifications(cursor);
        }
      },
      { threshold: 0.1 }
    );

    const currentTarget = observerTarget.current;
    if (currentTarget) {
      observer.observe(currentTarget);
    }

    return () => {
      if (currentTarget) {
        observer.unobserve(currentTarget);
      }
    };
  }, [hasMore, loadingMore, loading, cursor, fetchMoreNotifications]);

  const getNotificationIcon = (kind: string) => {
    switch (kind) {
      case 'like':
        return '♥';
      case 'reply':
        return '💬';
      case 'repost':
        return '🔄';
      case 'mention':
        return '@';
      default:
        return '🔔';
    }
  };

  const getNotificationText = (notif: Notification) => {
    switch (notif.kind) {
      case 'like':
        return 'liked your post';
      case 'reply':
        return 'replied to your post';
      case 'repost':
        return 'reposted your post';
      case 'mention':
        return 'mentioned you in a post';
      default:
        return 'interacted with your post';
    }
  };

  const getNotificationLink = (notif: Notification) => {
    // For replies and mentions, link to the post
    // For likes and reposts, we could link to the original post but we don't have it easily
    if (notif.kind === 'reply' || notif.kind === 'mention') {
      return getPostUrl(notif.source);
    }
    return null;
  };

  if (loading) {
    return (
      <div className="notifications-page">
        <div className="notifications-header">
          <h1>Notifications</h1>
        </div>
        <div className="loading">Loading notifications...</div>
      </div>
    );
  }

  if (error && notifications.length === 0) {
    return (
      <div className="notifications-page">
        <div className="notifications-header">
          <h1>Notifications</h1>
        </div>
        <div className="error">Error: {error}</div>
      </div>
    );
  }

  return (
    <div className="notifications-page">
      <div className="notifications-header">
        <h1>Notifications</h1>
      </div>
      <div className="notifications-list">
        {notifications.map((notif) => {
          const link = getNotificationLink(notif);
          const content = (
            <>
              <div className="notification-icon">
                {getNotificationIcon(notif.kind)}
              </div>
              <div className="notification-content">
                <div className="notification-author">
                  {notif.author.profile?.avatar && (
                    <img
                      src={getBlobUrl(notif.author.profile.avatar, notif.author.did, 'avatar_thumbnail')}
                      alt="Avatar"
                      className="notification-avatar"
                    />
                  )}
                  <div className="notification-text">
                    <Link to={`/profile/${notif.author.handle}`} className="notification-author-name">
                      {notif.author.profile?.displayName || notif.author.handle}
                    </Link>
                    {' '}
                    <span className="notification-action">{getNotificationText(notif)}</span>
                  </div>
                </div>
                {notif.sourcePost && (
                  <div className="notification-preview">
                    {notif.sourcePost.text}
                  </div>
                )}
                <div className="notification-time">
                  {formatRelativeTime(notif.createdAt)}
                </div>
              </div>
            </>
          );

          return (
            <div key={notif.id} className={`notification notification-${notif.kind}`}>
              {link ? (
                <Link to={link} className="notification-link">
                  {content}
                </Link>
              ) : (
                <div className="notification-inner">
                  {content}
                </div>
              )}
            </div>
          );
        })}
        {notifications.length === 0 && !loading && (
          <div className="empty-notifications">
            <p>No notifications yet</p>
          </div>
        )}
        {hasMore && (
          <div ref={observerTarget} className="load-more-trigger">
            {loadingMore && <div className="loading-more">Loading more...</div>}
          </div>
        )}
        {!hasMore && notifications.length > 0 && (
          <div className="end-of-notifications">
            <p>You're all caught up!</p>
          </div>
        )}
      </div>
    </div>
  );
};

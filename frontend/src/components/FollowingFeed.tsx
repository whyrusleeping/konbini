import React, { useState, useEffect, useRef } from 'react';
import { PostResponse } from '../types';
import { ApiClient } from '../api';
import { PostCard } from './PostCard';
import './FollowingFeed.css';

export const FollowingFeed: React.FC = () => {
  const [posts, setPosts] = useState<PostResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);
  const observerTarget = useRef<HTMLDivElement>(null);

  const fetchFeed = async (cursorToUse?: string) => {
    try {
      if (cursorToUse) {
        setLoadingMore(true);
      } else {
        setLoading(true);
      }

      const feedData = await ApiClient.getFollowingFeed(cursorToUse || undefined);

      if (cursorToUse) {
        setPosts(prev => [...prev, ...feedData.posts]);
      } else {
        setPosts(feedData.posts);
      }

      setCursor(feedData.cursor || null);
      setHasMore(!!feedData.cursor && feedData.posts.length > 0);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load feed');
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  };

  useEffect(() => {
    fetchFeed();
  }, []);

  // Set up intersection observer for infinite scroll
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loadingMore && !loading) {
          if (cursor) {
            fetchFeed(cursor);
          }
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
  }, [hasMore, loadingMore, loading, cursor]);

  if (loading) {
    return (
      <div className="following-feed">
        <div className="feed-header">
          <h1>Following</h1>
        </div>
        <div className="loading">Loading your feed...</div>
      </div>
    );
  }

  if (error && posts.length === 0) {
    return (
      <div className="following-feed">
        <div className="feed-header">
          <h1>Following</h1>
        </div>
        <div className="error">Error: {error}</div>
      </div>
    );
  }

  return (
    <div className="following-feed">
      <div className="feed-header">
        <h1>Following</h1>
        <p>{posts.length} posts loaded</p>
      </div>
      <div className="feed-content">
        {posts.map((post, index) => (
          <PostCard key={post.uri || index} postResponse={post} />
        ))}
        {posts.length === 0 && !loading && (
          <div className="empty-feed">
            <p>No posts in your following feed</p>
          </div>
        )}
        {hasMore && (
          <div ref={observerTarget} className="load-more-trigger">
            {loadingMore && <div className="loading-more">Loading more posts...</div>}
          </div>
        )}
        {!hasMore && posts.length > 0 && (
          <div className="end-of-feed">
            <p>You've reached the end!</p>
          </div>
        )}
      </div>
    </div>
  );
};

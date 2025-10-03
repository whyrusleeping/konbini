import React, { useState, useEffect } from 'react';
import { PostResponse } from '../types';
import { ApiClient } from '../api';
import { PostCard } from './PostCard';
import './FollowingFeed.css';

export const FollowingFeed: React.FC = () => {
  const [posts, setPosts] = useState<PostResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchFeed = async () => {
      try {
        setLoading(true);
        const feedData = await ApiClient.getFollowingFeed();
        setPosts(feedData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load feed');
      } finally {
        setLoading(false);
      }
    };

    fetchFeed();
  }, []);

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

  if (error) {
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
        <p>{posts.length} recent posts</p>
      </div>
      <div className="feed-content">
        {posts.map((post, index) => (
          <PostCard key={post.uri || index} postResponse={post} />
        ))}
        {posts.length === 0 && (
          <div className="empty-feed">
            <p>No posts in your following feed</p>
          </div>
        )}
      </div>
    </div>
  );
};
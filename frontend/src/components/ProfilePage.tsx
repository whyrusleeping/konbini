import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { ActorProfile, PostResponse } from '../types';
import { ApiClient } from '../api';
import { PostCard } from './PostCard';
import { getBlobUrl, formatDate } from '../utils';
import './ProfilePage.css';

export const ProfilePage: React.FC = () => {
  const { account } = useParams<{ account: string }>();
  const [profile, setProfile] = useState<ActorProfile | null>(null);
  const [posts, setPosts] = useState<PostResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userDid, setUserDid] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);
  const [activeTab, setActiveTab] = useState<'posts' | 'replies'>('posts');
  const observerTarget = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Scroll to top when navigating to a profile
    window.scrollTo(0, 0);

    const fetchProfile = async () => {
      if (!account) return;

      try {
        setLoading(true);
        setError(null);
        setPosts([]);
        setCursor(null);
        setHasMore(true);

        // Always try to load posts, regardless of profile status
        const postsPromise = ApiClient.getProfilePosts(account);
        const profilePromise = ApiClient.getProfile(account);

        const [profileData, postsData] = await Promise.all([
          profilePromise.catch(() => ({ error: 'Profile not found' })),
          postsPromise.catch(() => ({ posts: [], cursor: '' }))
        ]);

        if ('error' in profileData) {
          // Profile not found, but we can still show posts if available
          setProfile(null);
        } else {
          setProfile(profileData);
        }

        setPosts(postsData.posts || []);
        setCursor(postsData.cursor || null);
        setHasMore(!!(postsData.cursor && postsData.posts && postsData.posts.length > 0));

        // Extract DID from posts if available (posts include author info with DID)
        if (postsData.posts && postsData.posts.length > 0 && postsData.posts[0].author) {
          setUserDid(postsData.posts[0].author.did);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    };

    fetchProfile();
  }, [account]);

  const fetchMorePosts = useCallback(async (cursorToUse: string) => {
    if (!account || loadingMore || !hasMore) return;

    try {
      setLoadingMore(true);
      const data = await ApiClient.getProfilePosts(account, cursorToUse);
      setPosts(prev => [...prev, ...data.posts]);
      setCursor(data.cursor || null);
      setHasMore(!!(data.cursor && data.posts.length > 0));
    } catch (err) {
      console.error('Failed to fetch more posts:', err);
    } finally {
      setLoadingMore(false);
    }
  }, [account, loadingMore, hasMore]);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loadingMore && !loading && cursor) {
          fetchMorePosts(cursor);
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
  }, [hasMore, loadingMore, loading, cursor, fetchMorePosts]);

  if (loading) {
    return (
      <div className="profile-page">
        <div className="loading">Loading profile...</div>
      </div>
    );
  }

  if (error && posts.length === 0) {
    return (
      <div className="profile-page">
        <div className="error">
          {error}
        </div>
      </div>
    );
  }

  const hasBanner = !!(profile?.banner && userDid);

  return (
    <div className="profile-page">
      <div className={`profile-header ${!hasBanner ? 'profile-header--no-banner' : ''}`}>
        {hasBanner && profile.banner && (
          <div className="profile-banner">
            <img src={getBlobUrl(profile.banner, userDid!, 'feed_thumbnail')} alt="Profile banner" />
          </div>
        )}

        <div className="profile-info">
          <div className="profile-avatar-section">
            {profile?.avatar && userDid ? (
              <div className="profile-avatar">
                <img src={getBlobUrl(profile.avatar, userDid!, 'avatar_thumbnail')} alt="Profile avatar" />
              </div>
            ) : (
              <div className="profile-avatar profile-avatar--placeholder">
                <div className="avatar-placeholder-large">
                  {(profile?.displayName || account || 'U').charAt(0).toUpperCase()}
                </div>
              </div>
            )}
          </div>

          <div className="profile-details">
            <h1 className="profile-name">
              {profile?.displayName || account || 'Unknown User'}
            </h1>
            <p className="profile-handle">
              {account?.startsWith('did:') ? account : `@${account}`}
            </p>

            {profile && profile.description && (
              <div className="profile-description">
                {profile.description.split('\n').map((line, index) => (
                  <p key={index}>{line}</p>
                ))}
              </div>
            )}

            {!profile && (
              <div className="profile-description">
                <p className="profile-no-info">No profile information available</p>
              </div>
            )}

            <div className="profile-meta">
              {profile && profile.createdAt ? (
                <span>Joined {formatDate(profile.createdAt)}</span>
              ) : (
                <span>Profile information not available</span>
              )}
            </div>

            {profile && profile.pinnedPost && (
              <div className="pinned-post-notice">
                📌 Has pinned post
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="profile-content">
        <div className="profile-tabs">
          <button
            className={`profile-tab ${activeTab === 'posts' ? 'profile-tab--active' : ''}`}
            onClick={() => setActiveTab('posts')}
          >
            Posts
          </button>
          <button
            className={`profile-tab ${activeTab === 'replies' ? 'profile-tab--active' : ''}`}
            onClick={() => setActiveTab('replies')}
          >
            Replies
          </button>
        </div>

        <div className="posts-list">
          {posts
            .filter(post => activeTab === 'posts' ? !post.replyTo : !!post.replyTo)
            .map((post, index) => (
              <PostCard key={post.uri || index} postResponse={post} />
            ))}
          {posts.filter(post => activeTab === 'posts' ? !post.replyTo : !!post.replyTo).length === 0 && !loading && (
            <div className="empty-posts">
              <p>{activeTab === 'posts' ? 'No posts yet' : 'No replies yet'}</p>
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
    </div>
  );
};
import React, { useState, useEffect, useRef } from 'react';
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

  const fetchMorePosts = async (cursor: string) => {
    if (!account || loadingMore || !hasMore) return;

    try {
      setLoadingMore(true);
      const data = await ApiClient.getProfilePosts(account, cursor);
      setPosts(prev => [...prev, ...data.posts]);
      setCursor(data.cursor || null);
      setHasMore(!!(data.cursor && data.posts.length > 0));
    } catch (err) {
      console.error('Failed to fetch more posts:', err);
    } finally {
      setLoadingMore(false);
    }
  };

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loadingMore && !loading) {
          if (cursor) {
            fetchMorePosts(cursor);
          }
        }
      },
      { threshold: 0.1 }
    );

    if (observerTarget.current) {
      observer.observe(observerTarget.current);
    }

    return () => {
      if (observerTarget.current) {
        observer.unobserve(observerTarget.current);
      }
    };
  }, [hasMore, loadingMore, loading, cursor]);

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
        <div className="posts-header">
          <h2>Posts ({posts.length})</h2>
        </div>

        <div className="posts-list">
          {posts.map((post, index) => (
            <PostCard key={post.uri || index} postResponse={post} />
          ))}
          {posts.length === 0 && !loading && (
            <div className="empty-posts">
              <p>No posts yet</p>
            </div>
          )}
          {hasMore && <div ref={observerTarget} style={{ height: '20px' }} />}
          {loadingMore && (
            <div className="loading-more">Loading more posts...</div>
          )}
        </div>
      </div>
    </div>
  );
};
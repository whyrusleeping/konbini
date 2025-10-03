import React, { useState, useEffect } from 'react';
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
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchProfile = async () => {
      if (!account) return;

      try {
        setLoading(true);
        setError(null);

        // Always try to load posts, regardless of profile status
        const postsPromise = ApiClient.getProfilePosts(account);
        const profilePromise = ApiClient.getProfile(account);

        const [profileData, postsData] = await Promise.all([
          profilePromise.catch(() => ({ error: 'Profile not found' })),
          postsPromise.catch(() => [])
        ]);

        if ('error' in profileData) {
          // Profile not found, but we can still show posts if available
          setProfile(null);
        } else {
          setProfile(profileData);
        }

        setPosts(Array.isArray(postsData) ? postsData : []);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    };

    fetchProfile();
  }, [account]);

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

  return (
    <div className="profile-page">
      <div className="profile-header">
        {profile && profile.banner && (
          <div className="profile-banner">
            <img src={getBlobUrl(profile.banner, account, 'feed_thumbnail')} alt="Profile banner" />
          </div>
        )}

        <div className="profile-info">
          <div className="profile-avatar-section">
            {profile && profile.avatar ? (
              <div className="profile-avatar">
                <img src={getBlobUrl(profile.avatar, account, 'avatar_thumbnail')} alt="Profile avatar" />
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
          {posts.length === 0 && (
            <div className="empty-posts">
              <p>No posts yet</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
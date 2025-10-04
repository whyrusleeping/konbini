import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { EngagementUser } from '../types';
import { ApiClient } from '../api';
import { getBlobUrl, getProfileUrl, formatRelativeTime } from '../utils';
import './EngagementModal.css';

interface EngagementModalProps {
  postId: number;
  type: 'likes' | 'reposts' | 'replies';
  onClose: () => void;
}

export const EngagementModal: React.FC<EngagementModalProps> = ({ postId, type, onClose }) => {
  const [users, setUsers] = useState<EngagementUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchEngagement = async () => {
      try {
        setLoading(true);
        setError(null);
        let data;

        switch (type) {
          case 'likes':
            data = await ApiClient.getPostLikes(postId);
            break;
          case 'reposts':
            data = await ApiClient.getPostReposts(postId);
            break;
          case 'replies':
            data = await ApiClient.getPostReplies(postId);
            break;
        }

        setUsers(data.users);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    };

    fetchEngagement();
  }, [postId, type]);

  const getTitle = () => {
    switch (type) {
      case 'likes':
        return 'Liked by';
      case 'reposts':
        return 'Reposted by';
      case 'replies':
        return 'Replied by';
    }
  };

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  return (
    <div className="engagement-modal-backdrop" onClick={handleBackdropClick}>
      <div className="engagement-modal">
        <div className="engagement-modal-header">
          <h2>{getTitle()}</h2>
          <button className="modal-close-btn" onClick={onClose}>×</button>
        </div>

        <div className="engagement-modal-content">
          {loading && <div className="modal-loading">Loading...</div>}

          {error && <div className="modal-error">{error}</div>}

          {!loading && !error && users.length === 0 && (
            <div className="modal-empty">No {type} yet</div>
          )}

          {!loading && !error && users.length > 0 && (
            <div className="engagement-users-list">
              {users.map((user, index) => (
                <Link
                  key={`${user.did}-${index}`}
                  to={getProfileUrl(user.handle)}
                  className="engagement-user-item"
                  onClick={onClose}
                >
                  <div className="engagement-user-avatar">
                    {user.profile?.avatar ? (
                      <img
                        src={getBlobUrl(user.profile.avatar, user.did, 'avatar_thumbnail')}
                        alt={`${user.handle}'s avatar`}
                        className="user-avatar-img"
                      />
                    ) : (
                      <div className="user-avatar-placeholder">
                        {user.handle.charAt(0).toUpperCase()}
                      </div>
                    )}
                  </div>
                  <div className="engagement-user-info">
                    <div className="user-display-name">
                      {user.profile?.displayName || user.handle}
                    </div>
                    <div className="user-handle">@{user.handle}</div>
                    {user.profile?.description && (
                      <div className="user-bio">{user.profile.description}</div>
                    )}
                  </div>
                  <div className="engagement-time">
                    {formatRelativeTime(user.time)}
                  </div>
                </Link>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

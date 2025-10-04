import React, { useState } from 'react';
import { FeedPost, PostResponse } from '../types';
import { formatRelativeTime, getBlobUrl, getPostUrl, getProfileUrl, parseAtUri } from '../utils';
import { Link } from 'react-router-dom';
import { EngagementModal } from './EngagementModal';
import { ApiClient } from '../api';
import './PostCard.css';

interface PostCardProps {
  postResponse: PostResponse;
  showThreadIndicator?: boolean;
}

export const PostCard: React.FC<PostCardProps> = ({ postResponse, showThreadIndicator = true }) => {
  const [showEngagementModal, setShowEngagementModal] = useState<'likes' | 'reposts' | 'replies' | null>(null);
  const [isLiking, setIsLiking] = useState(false);
  const [localLikeCount, setLocalLikeCount] = useState(postResponse.counts?.likes || 0);
  const [isLiked, setIsLiked] = useState(!!postResponse.viewerLike);

  if (postResponse.missing || !postResponse.post) {
    return (
      <div className="post-card post-card--missing">
        <p>Post unavailable</p>
        <small>{postResponse.uri}</small>
      </div>
    );
  }

  const post = postResponse.post;
  const postUri = parseAtUri(postResponse.uri);
  const authorDid = postUri?.did;

  const handleLike = async (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();

    if (isLiking) return;

    console.log('Liking post:', { uri: postResponse.uri, cid: postResponse.cid });

    if (!postResponse.cid) {
      console.error('Post CID is missing!');
      alert('Cannot like post: missing CID');
      return;
    }

    setIsLiking(true);
    try {
      await ApiClient.likePost(postResponse.uri, postResponse.cid);
      setLocalLikeCount(prev => prev + 1);
      setIsLiked(true);
    } catch (err) {
      console.error('Failed to like post:', err);
      alert('Failed to like post. Please try again.');
    } finally {
      setIsLiking(false);
    }
  };

  const renderEmbed = (post: FeedPost) => {
    if (!post.embed) return null;

    switch (post.embed.$type) {
      case 'app.bsky.embed.images':
        return (
          <div className="post-embed post-embed--images">
            {post.embed.images.map((img, idx) => (
              <img
                key={idx}
                src={getBlobUrl(img.image, authorDid, 'feed_thumbnail')}
                alt={img.alt}
                className="post-image"
              />
            ))}
          </div>
        );

      case 'app.bsky.embed.external':
        return (
          <div className="post-embed post-embed--external">
            <a href={post.embed.external.uri} target="_blank" rel="noopener noreferrer" className="external-link">
              {post.embed.external.thumb && (
                <img src={getBlobUrl(post.embed.external.thumb, authorDid, 'feed_thumbnail')} alt="" className="external-thumb" />
              )}
              <div className="external-content">
                <h4>{post.embed.external.title}</h4>
                <p>{post.embed.external.description}</p>
                <small>{new URL(post.embed.external.uri).hostname}</small>
              </div>
            </a>
          </div>
        );

      case 'app.bsky.embed.record':
        const quotedRecord = post.embed.record;
        // Check if we have the full record view with author and value
        if ('author' in quotedRecord && 'value' in quotedRecord && quotedRecord.author && quotedRecord.value) {
          const quotedAuthor = quotedRecord.author;
          const quotedPost = quotedRecord.value;

          return (
            <div className="post-embed post-embed--record">
              <div className="quoted-post">
                <div className="quoted-post-header">
                  <Link to={getProfileUrl(quotedAuthor.handle)} className="quoted-author-link">
                    <div className="quoted-author-avatar">
                      {quotedAuthor.profile?.avatar ? (
                        <img
                          src={getBlobUrl(quotedAuthor.profile.avatar, quotedAuthor.did, 'avatar_thumbnail')}
                          alt="Author avatar"
                          className="avatar-image-small"
                        />
                      ) : (
                        <div className="avatar-placeholder-small">
                          {quotedAuthor.handle.charAt(0).toUpperCase()}
                        </div>
                      )}
                    </div>
                    <div className="quoted-author-info">
                      <span className="quoted-author-name">
                        {quotedAuthor.profile?.displayName || quotedAuthor.handle}
                      </span>
                      <span className="quoted-author-handle">@{quotedAuthor.handle}</span>
                    </div>
                  </Link>
                </div>
                <div className="quoted-post-content">
                  <p className="quoted-post-text">{quotedPost.text}</p>
                </div>
              </div>
            </div>
          );
        }

        // Fallback if we don't have the quoted post in our database
        return (
          <div className="post-embed post-embed--record">
            <div className="quoted-post-missing">
              <span className="missing-icon">📝</span>
              <span className="missing-text">Quoted post not available</span>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="post-card">
      {postResponse.author && (
        <div className="post-author">
          <Link to={getProfileUrl(postResponse.author.handle)} className="author-link">
            <div className="author-avatar">
              {postResponse.author.profile?.avatar ? (
                <img
                  src={getBlobUrl(postResponse.author.profile.avatar, postResponse.author.did, 'avatar_thumbnail')}
                  alt="Author avatar"
                  className="avatar-image"
                />
              ) : (
                <div className="avatar-placeholder">
                  {postResponse.author.handle.charAt(0).toUpperCase()}
                </div>
              )}
            </div>
            <div className="author-info">
              <span className="author-display-name">
                {postResponse.author.profile?.displayName || postResponse.author.handle}
              </span>
              <span className="author-handle">@{postResponse.author.handle}</span>
            </div>
          </Link>
          <time className="post-time">{formatRelativeTime(post.createdAt)}</time>
        </div>
      )}

      <div className="post-content">
        <Link to={getPostUrl(postResponse.uri)} className="post-text-link">
          <p className="post-text">{post.text}</p>
        </Link>
        {renderEmbed(post)}
      </div>

      {postResponse.counts && (
        <div className="post-engagement">
          <div className="engagement-stats">
            <button
              className={`stat-item stat-item-clickable ${isLiked ? 'stat-item-liked' : ''}`}
              onClick={handleLike}
              disabled={isLiking}
              title="Like this post"
            >
              <span className={`stat-icon ${isLiked ? 'stat-icon-liked' : 'stat-icon-unliked'}`}>♥</span>
              <span className="stat-count">{localLikeCount}</span>
            </button>
            <button
              className="stat-item stat-item-clickable"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setShowEngagementModal('reposts');
              }}
              disabled={postResponse.counts.reposts === 0}
            >
              <span className="stat-icon">🔄</span>
              <span className="stat-count">{postResponse.counts.reposts}</span>
            </button>
            <button
              className="stat-item stat-item-clickable"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setShowEngagementModal('replies');
              }}
              disabled={postResponse.counts.replies === 0}
            >
              <span className="stat-icon">💬</span>
              <span className="stat-count">{postResponse.counts.replies}</span>
            </button>
          </div>
        </div>
      )}

      {showEngagementModal && (
        <EngagementModal
          postId={postResponse.id}
          type={showEngagementModal}
          onClose={() => setShowEngagementModal(null)}
        />
      )}
    </div>
  );
};
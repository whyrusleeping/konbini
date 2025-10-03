import React from 'react';
import { FeedPost, PostResponse } from '../types';
import { formatRelativeTime, getBlobUrl, getPostUrl, getProfileUrl, parseAtUri } from '../utils';
import { Link } from 'react-router-dom';
import './PostCard.css';

interface PostCardProps {
  postResponse: PostResponse;
}

export const PostCard: React.FC<PostCardProps> = ({ postResponse }) => {
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
        return (
          <div className="post-embed post-embed--record">
            <p>Quoted post: {post.embed.record.uri}</p>
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
          <Link to={getProfileUrl(postResponse.author.did)} className="author-link">
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

      <Link to={getPostUrl(postResponse.uri)} className="post-content-link">
        <div className="post-content">
          <p className="post-text">{post.text}</p>
          {renderEmbed(post)}
        </div>
        <div className="post-meta">
          {post.langs && post.langs.length > 0 && (
            <span className="post-langs">
              {post.langs.join(', ')}
            </span>
          )}
        </div>
      </Link>

      {postResponse.counts && (
        <div className="post-engagement">
          <div className="engagement-stats">
            <span className="stat-item">
              <span className="stat-icon">♥</span>
              <span className="stat-count">{postResponse.counts.likes}</span>
            </span>
            <span className="stat-item">
              <span className="stat-icon">🔄</span>
              <span className="stat-count">{postResponse.counts.reposts}</span>
            </span>
            <span className="stat-item">
              <span className="stat-icon">💬</span>
              <span className="stat-count">{postResponse.counts.replies}</span>
            </span>
          </div>
        </div>
      )}
    </div>
  );
};
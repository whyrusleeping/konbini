import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { FeedPost } from '../types';
import { ApiClient } from '../api';
import { formatDate, getBlobUrl, parseAtUri, getProfileUrl } from '../utils';
import './PostView.css';

export const PostView: React.FC = () => {
  const { account, rkey } = useParams<{ account: string; rkey: string }>();
  const [post, setPost] = useState<FeedPost | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchPost = async () => {
      if (!account || !rkey) return;

      try {
        setLoading(true);
        setError(null);

        const postData = await ApiClient.getPost(account, rkey);
        setPost(postData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load post');
      } finally {
        setLoading(false);
      }
    };

    fetchPost();
  }, [account, rkey]);

  const renderEmbed = (post: FeedPost) => {
    if (!post.embed) return null;

    switch (post.embed.$type) {
      case 'app.bsky.embed.images':
        return (
          <div className="post-embed post-embed--images">
            {post.embed.images.map((img, idx) => (
              <div key={idx} className="image-container">
                <img
                  src={getBlobUrl(img.image, account, 'feed_thumbnail')}
                  alt={img.alt}
                  className="post-image"
                />
                {img.alt && <p className="image-alt">{img.alt}</p>}
              </div>
            ))}
          </div>
        );

      case 'app.bsky.embed.external':
        return (
          <div className="post-embed post-embed--external">
            <a href={post.embed.external.uri} target="_blank" rel="noopener noreferrer" className="external-link">
              {post.embed.external.thumb && (
                <img src={getBlobUrl(post.embed.external.thumb, account, 'feed_thumbnail')} alt="" className="external-thumb" />
              )}
              <div className="external-content">
                <h3>{post.embed.external.title}</h3>
                <p>{post.embed.external.description}</p>
                <small>{post.embed.external.uri}</small>
              </div>
            </a>
          </div>
        );

      case 'app.bsky.embed.record':
        const quoted = parseAtUri(post.embed.record.uri);
        return (
          <div className="post-embed post-embed--record">
            <div className="quoted-post">
              <p>Quoted post:</p>
              <Link to={`/profile/${quoted?.did}/post/${quoted?.rkey}`} className="quoted-link">
                {post.embed.record.uri}
              </Link>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  if (loading) {
    return (
      <div className="post-view">
        <div className="loading">Loading post...</div>
      </div>
    );
  }

  if (error || !post) {
    return (
      <div className="post-view">
        <div className="post-header">
          <Link to="/" className="back-link">← Back</Link>
        </div>
        <div className="error">
          {error || 'Post not found'}
        </div>
      </div>
    );
  }

  return (
    <div className="post-view">
      <div className="post-header">
        <Link to="/" className="back-link">← Back</Link>
        <h1>Post</h1>
      </div>

      <div className="post-content">
        <div className="post-author">
          <Link to={getProfileUrl(account!)} className="author-link">
            @{account}
          </Link>
        </div>

        <div className="post-main">
          <p className="post-text">{post.text}</p>
          {renderEmbed(post)}
        </div>

        <div className="post-meta">
          <time className="post-time" dateTime={post.createdAt}>
            {formatDate(post.createdAt)}
          </time>
          {post.langs && post.langs.length > 0 && (
            <div className="post-langs">
              Languages: {post.langs.join(', ')}
            </div>
          )}
        </div>

        <div className="post-uri">
          <small>at://{account}/app.bsky.feed.post/{rkey}</small>
        </div>
      </div>
    </div>
  );
};
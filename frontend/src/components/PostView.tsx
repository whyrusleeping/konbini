import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { PostResponse } from '../types';
import { ApiClient } from '../api';
import { PostCard } from './PostCard';
import { EngagementModal } from './EngagementModal';
import './PostView.css';

export const PostView: React.FC = () => {
  const { account, rkey } = useParams<{ account: string; rkey: string }>();
  const [mainPost, setMainPost] = useState<PostResponse | null>(null);
  const [threadPosts, setThreadPosts] = useState<PostResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showEngagementModal, setShowEngagementModal] = useState<'likes' | 'reposts' | 'replies' | null>(null);

  useEffect(() => {
    // Scroll to top when navigating to a post
    window.scrollTo(0, 0);

    const fetchPostAndThread = async () => {
      if (!account || !rkey) return;

      try {
        setLoading(true);
        setError(null);

        // First, get all posts from the profile to find this specific post
        const profilePostsData = await ApiClient.getProfilePosts(account);
        const targetPost = profilePostsData.posts.find(p => {
          const uriParts = p.uri.split('/');
          return uriParts[uriParts.length - 1] === rkey;
        });

        if (!targetPost) {
          setError('Post not found');
          setLoading(false);
          return;
        }

        setMainPost(targetPost);

        // If this post has replies or is part of a thread, fetch the thread
        if (targetPost.counts && targetPost.counts.replies > 0) {
          try {
            const threadData = await ApiClient.getThread(targetPost.id);
            // Filter out the main post and only show replies
            const replies = threadData.posts.filter(p => p.id !== targetPost.id);
            setThreadPosts(replies);
          } catch (err) {
            console.error('Failed to load thread:', err);
            // Don't fail if thread loading fails
          }
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load post');
      } finally {
        setLoading(false);
      }
    };

    fetchPostAndThread();
  }, [account, rkey]);

  if (loading) {
    return (
      <div className="post-view">
        <div className="post-view-header">
          <Link to="/" className="back-link">← Back</Link>
        </div>
        <div className="loading">Loading post...</div>
      </div>
    );
  }

  if (error || !mainPost) {
    return (
      <div className="post-view">
        <div className="post-view-header">
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
      <div className="post-view-header">
        <Link to="/" className="back-link">← Back</Link>
        <h1>Post</h1>
      </div>

      <div className="post-view-content">
        <div className="main-post">
          <PostCard postResponse={mainPost} showThreadIndicator={false} />
        </div>

        {mainPost.counts && (mainPost.counts.likes > 0 || mainPost.counts.reposts > 0 || mainPost.counts.replies > 0) && (
          <div className="post-engagement-detail">
            {mainPost.counts.likes > 0 && (
              <button
                className="engagement-detail-item"
                onClick={() => setShowEngagementModal('likes')}
              >
                <span className="engagement-detail-count">{mainPost.counts.likes}</span>
                <span className="engagement-detail-label">{mainPost.counts.likes === 1 ? 'Like' : 'Likes'}</span>
              </button>
            )}
            {mainPost.counts.reposts > 0 && (
              <button
                className="engagement-detail-item"
                onClick={() => setShowEngagementModal('reposts')}
              >
                <span className="engagement-detail-count">{mainPost.counts.reposts}</span>
                <span className="engagement-detail-label">{mainPost.counts.reposts === 1 ? 'Repost' : 'Reposts'}</span>
              </button>
            )}
            {mainPost.counts.replies > 0 && (
              <button
                className="engagement-detail-item"
                onClick={() => setShowEngagementModal('replies')}
              >
                <span className="engagement-detail-count">{mainPost.counts.replies}</span>
                <span className="engagement-detail-label">{mainPost.counts.replies === 1 ? 'Reply' : 'Replies'}</span>
              </button>
            )}
          </div>
        )}

        {threadPosts.length > 0 && (
          <div className="thread-replies">
            <div className="replies-header">
              <h2>Replies</h2>
            </div>
            {threadPosts.map((post, index) => (
              <div key={post.uri || index} className="thread-reply">
                <PostCard postResponse={post} showThreadIndicator={false} />
              </div>
            ))}
          </div>
        )}
      </div>

      {showEngagementModal && (
        <EngagementModal
          postId={mainPost.id}
          type={showEngagementModal}
          onClose={() => setShowEngagementModal(null)}
        />
      )}
    </div>
  );
};

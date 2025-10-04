import React, { useState, useEffect } from 'react';
import { useParams, useSearchParams, Link } from 'react-router-dom';
import { ThreadResponse, PostResponse } from '../types';
import { ApiClient } from '../api';
import { PostCard } from './PostCard';
import './ThreadView.css';

interface ThreadNode {
  post: PostResponse;
  replies: ThreadNode[];
}

export const ThreadView: React.FC = () => {
  const [searchParams] = useSearchParams();
  const postIdParam = searchParams.get('postId');
  const [threadData, setThreadData] = useState<ThreadResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Scroll to top when navigating to a thread
    window.scrollTo(0, 0);

    const fetchThread = async () => {
      if (!postIdParam) {
        setError('No post ID provided');
        setLoading(false);
        return;
      }

      try {
        setLoading(true);
        setError(null);
        const data = await ApiClient.getThread(parseInt(postIdParam));
        setThreadData(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load thread');
      } finally {
        setLoading(false);
      }
    };

    fetchThread();
  }, [postIdParam]);

  // Build a tree structure from flat posts array
  const buildThreadTree = (posts: PostResponse[]): ThreadNode[] => {
    const postMap = new Map<number, ThreadNode>();
    const roots: ThreadNode[] = [];

    // Create nodes for all posts
    posts.forEach(post => {
      const postId = extractPostId(post.uri);
      if (postId) {
        postMap.set(postId, { post, replies: [] });
      }
    });

    // Build the tree structure
    posts.forEach(post => {
      const postId = extractPostId(post.uri);
      if (!postId) return;

      const node = postMap.get(postId);
      if (!node) return;

      if (post.replyTo && post.replyTo !== 0) {
        const parentNode = postMap.get(post.replyTo);
        if (parentNode) {
          parentNode.replies.push(node);
        } else {
          // Parent not in thread, treat as root
          roots.push(node);
        }
      } else {
        // No parent, this is a root
        roots.push(node);
      }
    });

    return roots;
  };

  const extractPostId = (uri: string): number | null => {
    // Extract post ID from URI - we'll need to look it up somehow
    // For now, we'll rely on the posts being in order and having the inThread field
    const post = threadData?.posts.find(p => p.uri === uri);
    if (!post) return null;

    // We need a way to get the post ID - let's use a different approach
    // We'll match by checking if this is the root or using array index as fallback
    const index = threadData?.posts.indexOf(post);
    return index !== undefined ? index : null;
  };

  const renderThreadNode = (node: ThreadNode, depth: number = 0): React.ReactNode => {
    return (
      <div key={node.post.uri} className="thread-node" style={{ marginLeft: `${depth * 20}px` }}>
        <PostCard postResponse={node.post} showThreadIndicator={false} />
        {node.replies.length > 0 && (
          <div className="thread-replies">
            {node.replies.map(reply => renderThreadNode(reply, depth + 1))}
          </div>
        )}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="thread-view">
        <div className="thread-header">
          <Link to="/" className="back-link">← Back</Link>
          <h1>Thread</h1>
        </div>
        <div className="loading">Loading thread...</div>
      </div>
    );
  }

  if (error || !threadData) {
    return (
      <div className="thread-view">
        <div className="thread-header">
          <Link to="/" className="back-link">← Back</Link>
          <h1>Thread</h1>
        </div>
        <div className="error">{error || 'Failed to load thread'}</div>
      </div>
    );
  }

  // For now, let's just render posts in order since building the tree is complex without post IDs
  // We'll show them with indentation based on replyTo relationships
  const renderSimpleThread = () => {
    const rootPost = threadData.posts.find(p => p.inThread === 0 || !p.inThread);
    const replyPosts = threadData.posts.filter(p => p.inThread !== 0 && p.inThread);

    return (
      <div className="thread-content">
        {rootPost && (
          <div className="thread-root">
            <PostCard postResponse={rootPost} showThreadIndicator={false} />
          </div>
        )}
        {replyPosts.length > 0 && (
          <div className="thread-replies">
            {replyPosts.map((post, index) => (
              <div key={post.uri || index} className="thread-reply">
                <PostCard postResponse={post} showThreadIndicator={false} />
              </div>
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="thread-view">
      <div className="thread-header">
        <Link to="/" className="back-link">← Back</Link>
        <h1>Thread</h1>
        <p className="thread-info">{threadData.posts.length} posts in conversation</p>
      </div>
      {renderSimpleThread()}
    </div>
  );
};

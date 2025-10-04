import React, { useState } from 'react';
import { ApiClient } from '../api';
import './PostComposer.css';

interface PostComposerProps {
  onClose: () => void;
  onPostCreated?: () => void;
}

export const PostComposer: React.FC<PostComposerProps> = ({ onClose, onPostCreated }) => {
  const [text, setText] = useState('');
  const [isPosting, setIsPosting] = useState(false);

  const handlePost = async () => {
    if (!text.trim() || isPosting) return;

    setIsPosting(true);
    try {
      await ApiClient.createPost(text);
      onClose();
      if (onPostCreated) {
        onPostCreated();
      }
    } catch (err) {
      console.error('Failed to create post:', err);
      alert('Failed to create post. Please try again.');
    } finally {
      setIsPosting(false);
    }
  };

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  return (
    <div className="post-composer-backdrop" onClick={handleBackdropClick}>
      <div className="post-composer">
        <div className="post-composer-header">
          <h2>Create Post</h2>
          <button className="close-button" onClick={onClose}>×</button>
        </div>
        <div className="post-composer-body">
          <textarea
            className="post-composer-textarea"
            placeholder="What's happening?"
            value={text}
            onChange={(e) => setText(e.target.value)}
            autoFocus
            maxLength={300}
          />
          <div className="post-composer-footer">
            <span className="character-count">{text.length}/300</span>
            <button
              className="send-button"
              onClick={handlePost}
              disabled={!text.trim() || isPosting}
            >
              {isPosting ? 'Posting...' : 'Send'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

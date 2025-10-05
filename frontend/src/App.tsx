import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { FollowingFeed } from './components/FollowingFeed';
import { ProfilePage } from './components/ProfilePage';
import { PostView } from './components/PostView';
import { ThreadView } from './components/ThreadView';
import { PostComposer } from './components/PostComposer';
import { NotificationsPage } from './components/NotificationsPage';
import { ApiClient } from './api';
import './App.css';

function Navigation() {
  const location = useLocation();
  const [myHandle, setMyHandle] = useState<string | null>(null);

  useEffect(() => {
    ApiClient.getMe().then(data => {
      setMyHandle(data.handle);
    }).catch(err => {
      console.error('Failed to fetch current user:', err);
    });
  }, []);

  return (
    <nav className="app-nav">
      <div className="nav-container">
        <Link to="/" className="nav-brand">
          Konbini
        </Link>
        <div className="nav-links">
          <Link
            to="/"
            className={`nav-link ${location.pathname === '/' ? 'active' : ''}`}
          >
            Following
          </Link>
          <Link
            to="/notifications"
            className={`nav-link ${location.pathname === '/notifications' ? 'active' : ''}`}
          >
            Notifications
          </Link>
          {myHandle && (
            <Link
              to={`/profile/${myHandle}`}
              className={`nav-link ${location.pathname.includes('/profile/') ? 'active' : ''}`}
            >
              Profile
            </Link>
          )}
        </div>
      </div>
    </nav>
  );
}

function App() {
  const [showComposer, setShowComposer] = useState(false);

  return (
    <Router>
      <div className="app">
        <Navigation />
        <main className="app-main">
          <Routes>
            <Route path="/" element={<FollowingFeed />} />
            <Route path="/notifications" element={<NotificationsPage />} />
            <Route path="/profile/:account" element={<ProfilePage />} />
            <Route path="/profile/:account/post/:rkey" element={<PostView />} />
            <Route path="/thread" element={<ThreadView />} />
          </Routes>
        </main>
        <button className="floating-post-button" onClick={() => setShowComposer(true)}>
          POST
        </button>
        {showComposer && (
          <PostComposer
            onClose={() => setShowComposer(false)}
            onPostCreated={() => {
              window.location.reload();
            }}
          />
        )}
      </div>
    </Router>
  );
}

export default App;

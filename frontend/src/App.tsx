import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { FollowingFeed } from './components/FollowingFeed';
import { ProfilePage } from './components/ProfilePage';
import { PostView } from './components/PostView';
import { ThreadView } from './components/ThreadView';
import './App.css';

function Navigation() {
  const location = useLocation();

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
        </div>
      </div>
    </nav>
  );
}

function App() {
  return (
    <Router>
      <div className="app">
        <Navigation />
        <main className="app-main">
          <Routes>
            <Route path="/" element={<FollowingFeed />} />
            <Route path="/profile/:account" element={<ProfilePage />} />
            <Route path="/profile/:account/post/:rkey" element={<PostView />} />
            <Route path="/thread" element={<ThreadView />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;

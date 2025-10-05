import { PostResponse, ActorProfile, ApiError, ThreadResponse, EngagementResponse, FeedResponse, NotificationsResponse } from './types';

const API_BASE_URL = 'http://localhost:4444/api';

export class ApiClient {
  static async getMe(): Promise<{did: string, handle: string}> {
    const response = await fetch(`${API_BASE_URL}/me`);
    if (!response.ok) {
      throw new Error(`Failed to fetch current user: ${response.statusText}`);
    }
    return response.json();
  }

  static async getFollowingFeed(cursor?: string): Promise<FeedResponse> {
    const url = cursor
      ? `${API_BASE_URL}/followingfeed?cursor=${encodeURIComponent(cursor)}`
      : `${API_BASE_URL}/followingfeed`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch following feed: ${response.statusText}`);
    }
    return response.json();
  }

  static async getProfile(account: string): Promise<ActorProfile | ApiError> {
    const response = await fetch(`${API_BASE_URL}/profile/${encodeURIComponent(account)}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch profile: ${response.statusText}`);
    }
    return response.json();
  }

  static async getProfilePosts(account: string, cursor?: string): Promise<FeedResponse> {
    const url = cursor
      ? `${API_BASE_URL}/profile/${encodeURIComponent(account)}/posts?cursor=${encodeURIComponent(cursor)}`
      : `${API_BASE_URL}/profile/${encodeURIComponent(account)}/posts`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch profile posts: ${response.statusText}`);
    }
    return response.json();
  }

  static async getPost(account: string, rkey: string): Promise<any> {
    const response = await fetch(`${API_BASE_URL}/profile/${encodeURIComponent(account)}/post/${encodeURIComponent(rkey)}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch post: ${response.statusText}`);
    }
    return response.json();
  }

  static async getThread(postId: number): Promise<ThreadResponse> {
    const response = await fetch(`${API_BASE_URL}/thread/${postId}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch thread: ${response.statusText}`);
    }
    return response.json();
  }

  static async getPostLikes(postId: number): Promise<EngagementResponse> {
    const response = await fetch(`${API_BASE_URL}/post/${postId}/likes`);
    if (!response.ok) {
      throw new Error(`Failed to fetch likes: ${response.statusText}`);
    }
    return response.json();
  }

  static async getPostReposts(postId: number): Promise<EngagementResponse> {
    const response = await fetch(`${API_BASE_URL}/post/${postId}/reposts`);
    if (!response.ok) {
      throw new Error(`Failed to fetch reposts: ${response.statusText}`);
    }
    return response.json();
  }

  static async getPostReplies(postId: number): Promise<EngagementResponse> {
    const response = await fetch(`${API_BASE_URL}/post/${postId}/replies`);
    if (!response.ok) {
      throw new Error(`Failed to fetch replies: ${response.statusText}`);
    }
    return response.json();
  }

  static async createRecord(collection: string, record: Record<string, any>): Promise<{uri: string, cid: string}> {
    const response = await fetch(`${API_BASE_URL}/createRecord`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        collection,
        record,
      }),
    });
    if (!response.ok) {
      throw new Error(`Failed to create record: ${response.statusText}`);
    }
    return response.json();
  }

  static async likePost(postUri: string, postCid: string): Promise<{uri: string, cid: string}> {
    return this.createRecord('app.bsky.feed.like', {
      $type: 'app.bsky.feed.like',
      subject: {
        uri: postUri,
        cid: postCid,
      },
      createdAt: new Date().toISOString(),
    });
  }

  static async createPost(text: string): Promise<{uri: string, cid: string}> {
    return this.createRecord('app.bsky.feed.post', {
      $type: 'app.bsky.feed.post',
      text: text,
      createdAt: new Date().toISOString(),
    });
  }

  static async getNotifications(cursor?: string): Promise<NotificationsResponse> {
    const url = cursor
      ? `${API_BASE_URL}/notifications?cursor=${encodeURIComponent(cursor)}`
      : `${API_BASE_URL}/notifications`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch notifications: ${response.statusText}`);
    }
    return response.json();
  }
}
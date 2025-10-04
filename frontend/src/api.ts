import { PostResponse, ActorProfile, ApiError, ThreadResponse, EngagementResponse, FeedResponse } from './types';

const API_BASE_URL = 'http://localhost:4444/api';

export class ApiClient {
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

  static async getProfilePosts(account: string): Promise<PostResponse[]> {
    const response = await fetch(`${API_BASE_URL}/profile/${encodeURIComponent(account)}/posts`);
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
}
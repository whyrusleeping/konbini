import { PostResponse, ActorProfile, ApiError } from './types';

const API_BASE_URL = 'http://localhost:4444/api';

export class ApiClient {
  static async getFollowingFeed(): Promise<PostResponse[]> {
    const response = await fetch(`${API_BASE_URL}/followingfeed`);
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
}
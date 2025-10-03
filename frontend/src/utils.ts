import { BlobRef } from './types';

export function formatDate(dateString: string): string {
  const date = new Date(dateString);
  return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

export function formatRelativeTime(dateString: string): string {
  const date = new Date(dateString);
  const now = new Date();
  const diff = now.getTime() - date.getTime();

  const seconds = Math.floor(diff / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0) return `${days}d`;
  if (hours > 0) return `${hours}h`;
  if (minutes > 0) return `${minutes}m`;
  return `${seconds}s`;
}

export function getBlobUrl(blob: BlobRef, did?: string, type: 'avatar_thumbnail' | 'feed_thumbnail' = 'feed_thumbnail'): string {
  // Use Bluesky CDN format: https://cdn.bsky.app/img/{type}/plain/{did}/{cid}@jpeg
  const cid = blob.ref.$link;
  const didParam = did || 'unknown';
  return `https://cdn.bsky.app/img/${type}/plain/${didParam}/${cid}@jpeg`;
}

export function parseAtUri(uri: string): { did: string; collection: string; rkey: string } | null {
  const match = uri.match(/^at:\/\/([^/]+)\/([^/]+)\/(.+)$/);
  if (!match) return null;

  return {
    did: match[1],
    collection: match[2],
    rkey: match[3]
  };
}

export function getProfileUrl(did: string): string {
  return `/profile/${encodeURIComponent(did)}`;
}

export function getPostUrl(uri: string): string {
  const parsed = parseAtUri(uri);
  if (!parsed) return '#';

  return `/profile/${encodeURIComponent(parsed.did)}/post/${encodeURIComponent(parsed.rkey)}`;
}
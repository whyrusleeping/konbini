// ATProto/Bluesky data types
export interface BlobRef {
  $type: "blob";
  ref: {
    $link: string;
  };
  mimeType: string;
  size: number;
}

export interface FacetFeature {
  $type: string;
  tag?: string;
  did?: string;
  uri?: string;
}

export interface Facet {
  features: FacetFeature[];
  index: {
    byteStart: number;
    byteEnd: number;
  };
}

export interface EmbedImage {
  alt: string;
  aspectRatio?: {
    height: number;
    width: number;
  };
  image: BlobRef;
}

export interface EmbedImages {
  $type: "app.bsky.embed.images";
  images: EmbedImage[];
}

export interface EmbedExternal {
  $type: "app.bsky.embed.external";
  external: {
    description: string;
    thumb?: BlobRef;
    title: string;
    uri: string;
  };
}

export interface EmbedRecord {
  $type: "app.bsky.embed.record";
  record: {
    cid: string;
    uri: string;
  };
}

export type Embed = EmbedImages | EmbedExternal | EmbedRecord;

export interface FeedPost {
  $type: "app.bsky.feed.post";
  createdAt: string;
  langs?: string[];
  text: string;
  facets?: Facet[];
  embed?: Embed;
}

export interface AuthorInfo {
  handle: string;
  did: string;
  profile?: ActorProfile;
}

export interface PostCounts {
  likes: number;
  reposts: number;
  replies: number;
}

export interface PostResponse {
  missing: boolean;
  uri: string;
  post?: FeedPost;
  author?: AuthorInfo;
  counts?: PostCounts;
  id: number;
  replyTo?: number;
  replyToUsr?: number;
  inThread?: number;
}

export interface ThreadResponse {
  posts: PostResponse[];
  rootPostId: number;
}

export interface ActorProfile {
  $type: "app.bsky.actor.profile";
  avatar?: BlobRef;
  banner?: BlobRef;
  createdAt: string;
  description?: string;
  displayName?: string;
  pinnedPost?: {
    cid: string;
    uri: string;
  };
}

export interface ApiError {
  error: string;
}

export interface EngagementUser {
  handle: string;
  did: string;
  profile?: ActorProfile;
  time: string;
}

export interface EngagementResponse {
  users: EngagementUser[];
  count: number;
}

export interface FeedResponse {
  posts: PostResponse[];
  cursor: string;
}
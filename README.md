# Konbini - A Cozy Bluesky AppView

Konbini is a partially indexed bluesky appview. It's aim is to provide a "Friends of Friends" experience to the bluesky network.

It is currently _very_ jank and I really just hacked this together in a day. More work to come when I get time.

## Prerequisites

- Go 1.25.1 or later
- PostgreSQL database
- Node.js and npm (for frontend)
- Docker (optional, for easy PostgreSQL setup)
- Bluesky account credentials

## Quick Start with Docker Compose

The easiest way to run Konbini is with Docker Compose, which will start PostgreSQL, the backend, and frontend all together.

### Prerequisites

- Docker and Docker Compose installed
- Creating an app password (via: https://bsky.app/settings/app-passwords)

### Setup

1. Create a `.env` file with your credentials:

```bash
cp .env.example .env
# Edit .env and add:
# - BSKY_HANDLE=your-handle.bsky.social
# - BSKY_PASSWORD=your-app-password
```

2. Start all services:

```bash
docker-compose up -d
```

3. Wait for the backend to index posts from the firehose (this may take a few minutes for initial indexing)

4. Open your browser to http://localhost:3000

### Stopping the services

```bash
docker-compose down
```

To also remove the database volume:

```bash
docker-compose down -v
```

## Manual Setup

### 1. PostgreSQL Database Setup

#### Using Docker (Recommended)

```bash
# Start PostgreSQL container
docker run --name konbini-postgres \
  -e POSTGRES_DB=konbini \
  -e POSTGRES_USER=konbini \
  -e POSTGRES_PASSWORD=your_password \
  -p 5432:5432 \
  -d postgres:15

# The database will be available at: postgresql://konbini:your_password@localhost:5432/konbini
```

### 2. Environment Configuration

Set the following environment variables:

```bash
# Database connection
export DATABASE_URL="postgresql://konbini:your_password@localhost:5432/konbini"

# Bluesky credentials
export BSKY_HANDLE="your-handle.bsky.social"
export BSKY_PASSWORD="your-app-password"
```

### 3. Build and Run the Go Application

```bash
go build

# Run with environment variables
./konbini
```

### 4. Frontend Setup

```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
npm install

# Start the development server
npm start
```

The frontend will be available at http://localhost:3000 and will connect to the API at http://localhost:4444.

## Running the Bluesky App against Konbini

Konbini implements a large portion of the app.bsky.\* appview endpoints that
are required for pointing the main app to it and having it work reasonably
well.

To accomplish this you will need a few things:

### Service DID

You will need a DID, preferably a did:web for your appview that points at a
public endpoint where your appview is accessible via https.
I'll get into the https proxy next, but for the did, I've just pointed a domain
I own (in my case appview1.bluesky.day) to a VPS, and used caddy to host a file
at `/.well-known/did.json`.
That file should look like this:

```json
{
  "@context": [
    "https://www.w3.org/ns/did/v1",
    "https://w3id.org/security/multikey/v1"
  ],
  "id": "did:web:appview1.bluesky.day",
  "verificationMethod": [
    {
      "id": "did:web:api.bsky.app#atproto",
      "type": "Multikey",
      "controller": "did:web:api.bsky.app",
      "publicKeyMultibase": "zQ3shpRzb2NDriwCSSsce6EqGxG23kVktHZc57C3NEcuNy1jg"
    }
  ],
  "service": [
    {
      "id": "#bsky_notif",
      "type": "BskyNotificationService",
      "serviceEndpoint": "YOUR APPVIEW HTTPS URL"
    },
    {
      "id": "#bsky_appview",
      "type": "BskyAppView",
      "serviceEndpoint": "YOUR APPVIEW HTTPS URL"
    }
  ]
}
```

The verificationMethod isn't used but i'm not sure if _something_ is required
there or not, so i'm just leaving that there, it works on my machine.

### HTTPS Endpoint

I've been using ngrok to proxy traffic from a publicly accessible https url to my appview.
You can simply run `ngrok http 4446` and it will give you an https url that you
can then put in your DID doc above.

### The Social App

Now, clone and build the social app:

```
git clone https://github.com/bluesky-social/social-app
cd social-app
yarn
```

And then set this environment variable that tells it to use your appview:

```
export EXPO_PUBLIC_BLUESKY_PROXY_DID=did:web:YOURDIDWEB
```

And finally run the app:

```
yarn web
```

This takes a while on first load since its building everything.
After that, load the localhost url it gives you and it _should_ work.

## Selective Backfill

If you'd like to backfill a particular repo, just hit the following endpoint:

```
curl http://localhost:4444/rescan/<DID OR HANDLE>

```

It will take a minute but it should pull all records from that user.

## License

MIT (whyrusleeping)

```

```

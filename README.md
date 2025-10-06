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

## License

MIT (whyrusleeping)

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rcache is a two-tier caching system that combines Redis (hot cache) with PostgreSQL/MySQL (cold storage) through a DataProxy layer. It implements optimistic concurrency control using version numbers to prevent lost update problems.

```
Client → DataProxy → Redis (Hot Cache) ↔ PostgreSQL/MySQL (Cold Storage)
```

## Build and Test Commands

```bash
# Build the module
go build

# Run all tests
go test

# Run specific test
go test -run=Get
go test -run=SetWithVersion

# Run with coverage
go test -cover
go tool cover -html=coverage.out
```

## High-Level Architecture

### Core Components

- **dataproxy.go**: Main `DataProxy` struct orchestrating cache operations. Manages both Redis and database connections.
- **redis.go**: Redis operations using Lua scripts for atomicity. Uses a `__dirty__` hash to track modified entries needing sync.
- **sql.go**: Database operations for PostgreSQL and MySQL with version checking for optimistic concurrency.

### Cache Flow

1. **Get operation**: Try Redis first → cache miss triggers database query → load result into Redis → return value
2. **Set operation**: Update Redis → mark as dirty → async writeback to database
3. **Set cache miss**: Insert/update database → load into Redis → return

### Version Control System

Each key has an associated version number. The `SetWithVersion()` method ensures atomic updates only if the version matches, preventing concurrent modification conflicts.

### Dirty Tracking

Modified cache entries are tracked in a Redis hash key `__dirty__`. The `SyncDirtyToDB()` method writes dirty entries back to the database and clears the dirty flags using HScan with batch size of 100.

### Database Schema

```sql
CREATE TABLE kv (
    key VARCHAR PRIMARY KEY,
    value TEXT,
    version INTEGER
);
```

## Key Lua Scripts (redis.go)

- `scriptSet`: Atomic cache update with version checking, marks entry as dirty
- `scriptGet`: Retrieves cached value
- `scriptLoadGet`: Loads value from database into cache (used on cache miss)
- `scriptLoadSet`: Conditionally loads value if Redis key doesn't exist
- `scriptClearDirty`: Clears dirty flags after successful sync

## Development Notes

- Tests require local PostgreSQL on localhost:5432 and Redis on localhost:6379
- Default cache timeout is 1800 seconds (30 minutes)
- Uses `github.com/jmoiron/sqlx` for database operations
- Uses `github.com/redis/go-redis/v9` for Redis client

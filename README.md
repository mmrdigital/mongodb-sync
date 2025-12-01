# MongoDB Migration Tool - Database Sync & Collection Transfer

A distributed MongoDB migration and synchronization tool for seamlessly transferring collections, databases, and namespaces between MongoDB clusters with zero downtime. Perfect for MongoDB database migration, collection replication, cross-cluster sync, and MongoDB Atlas migrations.

## Features

**MongoDB Migration & Sync:**
- MongoDB collection migration between clusters
- Database namespace transfer and replication
- MongoDB Atlas migration support
- Cross-cluster data synchronization
- MongoDB database copy and clone operations

**Production-Ready:**
- Distributed coordinator-worker architecture for horizontal scaling
- Real-time streaming via MongoDB change streams
- Zero-downtime MongoDB migrations
- Automatic recovery and retry mechanisms
- Command-line monitoring and progress tracking
- Automatic index transfer and creation (including compound and text indexes)
- Docker deployment ready

## Requirements

**MongoDB Migration Requirements:**
- Node.js 18+
- MongoDB 4.4+ (source and destination clusters)
- MongoDB Atlas supported
- Supports standalone replicas and sharded clusters
- Docker (optional)
- Kubernetes (optional)

**Supported MongoDB Deployments:**
- MongoDB replica sets
- MongoDB sharded clusters
- MongoDB Atlas clusters
- Self-hosted MongoDB servers

## Configuration

Create a `config.json` file for your MongoDB migration:

```json
{
  "sourceUri": "mongodb://user:pass@source-cluster:27017",
  "destUri": "mongodb://user:pass@dest-cluster:27017",
  "sourceDatabase": "myapp",
  "destDatabase": "myapp_migrated",
  "progressDatabase": "sync_progress", 
  "collections": ["users", "orders", "products"],
  "batchSize": 1000,
  "maxRetries": 3,
  "retryDelay": 5000,
  "maxAcceptableLag": 2000,
  "workersPerCollection": 10
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sourceUri` | string | - | Source MongoDB connection string (supports MongoDB Atlas) |
| `destUri` | string | - | Destination MongoDB connection string (supports MongoDB Atlas) |
| `sourceDatabase` | string | - | Source database name to migrate |
| `destDatabase` | string | - | Destination database name (can be different namespace) |
| `progressDatabase` | string | `sync_progress` | Database for tracking migration progress |
| `collections` | string[] | - | Array of collection names to migrate/sync |
| `batchSize` | number | `1000` | Documents per batch for collection transfer |
| `maxRetries` | number | `3` | Maximum retry attempts for failed migration operations |
| `retryDelay` | number | `5000` | Delay between retries (ms) |
| `maxAcceptableLag` | number | `10000` | Maximum acceptable replication lag (ms) |
| `workersPerCollection` | number | `10` | Workers per collection for parallel migration |


## Usage

### Basic Usage

#### Start Coordinator
```bash
npm start -- --mode coordinator --config config.json
```

#### Start Workers
```bash
npm start -- --mode worker --config config.json
```

### Using Built-in Scripts

```bash
npm run start:coordinator

npm run start:worker

npm run dev:coordinator
npm run dev:worker
```
### Docker Usage

#### Using docker-compose
```bash
docker-compose up
```
## Architecture

### MongoDB Migration Process

1. **Collection Migration**: Historical data transferred in parallel batches
2. **Database Sync**: Real-time synchronization during migration
3. **Index Transfer**: Automatically creates all indexes from source collections, including compound indexes, text indexes, and custom index options
4. **Namespace Mapping**: Support for database name changes during migration

### Components

- **Migration Coordinator**: Orchestrates collection transfers and database sync
- **Migration Workers**: Execute parallel collection migration tasks
- **Progress Tracking**: Real-time migration status and progress monitoring
- **Command Line Monitoring**: Live migration monitoring via console logs

### Migration Types

- **Bulk Collection Transfer**: Batch migration of existing collection data
- **Incremental Sync**: Continuous replication of new/changed documents
- **Live Migration**: Zero-downtime database migration with real-time sync

## Monitoring

The console output provides real-time monitoring including system status, task progress, worker distribution, and lag metrics.

Logging levels can be configured via the `LOG_LEVEL` environment variable.

## Docker Deployment

### Dockerfile

The included Dockerfile creates an optimized production image:

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY dist/ ./dist/
EXPOSE 3000
CMD ["node", "dist/index.js"]
```

### Docker Compose

```yaml
version: '3.8'
services:
  coordinator:
    build: .
    command: ["node", "dist/index.js", "--mode", "coordinator"]
    ports:
      - "3000:3000"
    environment:
      - SOURCE_URI=mongodb://mongo-source:27017
      - DEST_URI=mongodb://mongo-dest:27017
      - SOURCE_DATABASE=myapp
      - DEST_DATABASE=myapp_synced
      - COLLECTIONS=users,orders
    
  worker:
    build: .
    command: ["node", "dist/index.js", "--mode", "worker"]
    environment:
      - SOURCE_URI=mongodb://mongo-source:27017  
      - DEST_URI=mongodb://mongo-dest:27017
      - SOURCE_DATABASE=myapp
      - DEST_DATABASE=myapp_synced
      - COLLECTIONS=users,orders
    deploy:
      replicas: 3
```
## Contributing

1. Fork the repository and create a feature branch
2. Add tests for new functionality
3. Ensure all tests pass with `npm test`
4. Run `npm run lint` to check code style
5. Submit a pull request

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Support

Report issues on [GitHub Issues](https://github.com/your-username/mongodb-sync-tool/issues).

---

**Keywords:** MongoDB migration, database migration, collection transfer, MongoDB sync, MongoDB Atlas migration, cross-cluster replication, database namespace migration, MongoDB backup, data migration tool, MongoDB replication, cluster migration, MongoDB copy database, collection sync, zero-downtime migration
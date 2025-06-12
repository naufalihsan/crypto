# Crypto Pipeline Management Scripts

This directory contains comprehensive management scripts for the Crypto Pipeline project. These scripts provide easy-to-use interfaces for starting, stopping, monitoring, and managing the entire pipeline infrastructure.

## Available Scripts

### 🚀 `start-pipeline.sh`
**Purpose**: Starts all services in the correct order with proper health checks and dependency management.

**Usage**:
```bash
./scripts/start-pipeline.sh [OPTIONS]
```

**Options**:
- `--dev`: Use development configuration with code mounting and debug ports
- `--build`: Force rebuild of custom Docker images
- `--no-pull`: Skip pulling latest images from registries
- `--cleanup`: Clean up existing containers before starting
- `--help`: Show help message

**Examples**:
```bash
# Start with default configuration
./scripts/start-pipeline.sh

# Start in development mode with code mounting
./scripts/start-pipeline.sh --dev

# Start with cleanup and rebuild
./scripts/start-pipeline.sh --cleanup --build

# Start without pulling latest images
./scripts/start-pipeline.sh --no-pull
```

**Features**:
- ✅ Proper service startup sequencing (Infrastructure → Processing → Applications → Monitoring)
- ✅ Health checks and dependency validation
- ✅ Automatic directory creation for data persistence
- ✅ Colored output with progress indicators
- ✅ Service URL display upon successful startup
- ✅ Error handling and rollback capabilities

---

### 🛑 `stop-pipeline.sh`
**Purpose**: Gracefully stops all services in reverse order with optional cleanup.

**Usage**:
```bash
./scripts/stop-pipeline.sh [OPTIONS]
```

**Options**:
- `--dev`: Use development configuration
- `--remove`: Remove containers after stopping
- `--volumes`: Remove volumes (⚠️ **WARNING**: This removes all data)
- `--images`: Remove custom Docker images
- `--clean-data`: Remove data directories (⚠️ **WARNING**: This removes all data)
- `--force`: Force stop without graceful shutdown
- `--help`: Show help message

**Examples**:
```bash
# Graceful shutdown
./scripts/stop-pipeline.sh

# Stop and remove containers
./scripts/stop-pipeline.sh --remove

# Complete cleanup (removes all data)
./scripts/stop-pipeline.sh --volumes --clean-data

# Force stop all services
./scripts/stop-pipeline.sh --force
```

**Features**:
- ✅ Graceful shutdown in reverse dependency order
- ✅ Optional container and volume cleanup
- ✅ Data preservation options
- ✅ Force stop for emergency situations
- ✅ Status verification after shutdown

---

### 📋 `logs.sh`
**Purpose**: Comprehensive log management with filtering, monitoring, and export capabilities.

**Usage**:
```bash
./scripts/logs.sh [COMMAND] [OPTIONS]
```

**Commands**:
- `show [SERVICE]`: Show logs for a service or all services
- `follow [SERVICE]`: Follow logs in real-time
- `errors [SERVICE]`: Show only error logs
- `filter PATTERN [SERVICE]`: Show logs matching pattern
- `export [SERVICE] [FILE]`: Export logs to file
- `stats [SERVICE]`: Show log statistics
- `monitor [SERVICE]`: Monitor logs with color highlighting
- `services`: List available services

**Options**:
- `-n, --lines NUM`: Number of lines to show (default: 100)
- `-f, --follow`: Follow logs in real-time
- `--dev`: Use development configuration
- `--help`: Show help message

**Examples**:
```bash
# Show recent logs for crypto-producer
./scripts/logs.sh show crypto-producer

# Follow Kafka logs in real-time
./scripts/logs.sh follow kafka

# Show only error logs from all services
./scripts/logs.sh errors

# Filter logs for specific patterns
./scripts/logs.sh filter "ERROR" crypto-producer

# Export producer logs to file
./scripts/logs.sh export crypto-producer logs/producer.log

# Show log statistics
./scripts/logs.sh stats

# Monitor all logs with color highlighting
./scripts/logs.sh monitor
```

**Features**:
- ✅ Service-specific and aggregate log viewing
- ✅ Real-time log following with color highlighting
- ✅ Pattern-based filtering with regex support
- ✅ Log export functionality
- ✅ Statistical analysis of log patterns
- ✅ Error and warning highlighting
- ✅ Support for all pipeline services

---

### 🏥 `health-check.sh`
**Purpose**: Comprehensive health monitoring and status checking for all services.

**Usage**:
```bash
./scripts/health-check.sh [COMMAND] [OPTIONS]
```

**Commands**:
- `check`: Check all services (default)
- `deps`: Check service dependencies
- `flow`: Check data flow
- `resources`: Check resource usage
- `monitor [INTERVAL]`: Monitor health continuously
- `report [FILE]`: Generate health report

**Options**:
- `--dev`: Use development configuration
- `--help`: Show help message

**Examples**:
```bash
# Check all services
./scripts/health-check.sh

# Check only dependencies
./scripts/health-check.sh deps

# Check data flow
./scripts/health-check.sh flow

# Monitor continuously every 60 seconds
./scripts/health-check.sh monitor 60

# Generate health report
./scripts/health-check.sh report
```

**Features**:
- ✅ Comprehensive service status checking
- ✅ Container health and port connectivity validation
- ✅ HTTP endpoint health checks
- ✅ Dependency chain validation
- ✅ Resource usage monitoring
- ✅ Continuous monitoring with auto-refresh
- ✅ Health report generation
- ✅ Color-coded status indicators

---

## Service Architecture

The scripts manage the following services in dependency order:

### Infrastructure Layer
1. **Zookeeper** (Port 2181) - Kafka coordination
2. **Kafka** (Port 9092) - Message streaming
3. **Schema Registry** (Port 8081) - Schema management
4. **PostgreSQL OLTP** (Port 5433) - Transactional database
5. **ClickHouse** (Port 8123) - Analytical database

### Processing Layer
6. **Flink JobManager** (Port 8082) - Stream processing coordination
7. **Flink TaskManager** - Stream processing execution

### Application Layer
8. **Crypto Producer** (Port 8000) - Data ingestion
9. **OLTP Connector** (Port 8001) - PostgreSQL data connector
10. **OLAP Connector** (Port 8002) - ClickHouse data connector

### Monitoring Layer
11. **Kafka UI** (Port 8080) - Kafka management interface
12. **Prometheus** (Port 9090) - Metrics collection
13. **Grafana** (Port 3000) - Metrics visualization

## Quick Start Guide

### 1. Start the Pipeline
```bash
# Development mode (recommended for first-time setup)
./scripts/start-pipeline.sh --dev --build

# Production mode
./scripts/start-pipeline.sh
```

### 2. Check Health
```bash
# Quick health check
./scripts/health-check.sh

# Detailed dependency check
./scripts/health-check.sh deps
```

### 3. Monitor Logs
```bash
# Monitor all services
./scripts/logs.sh monitor

# Follow specific service
./scripts/logs.sh follow crypto-producer
```

### 4. Stop the Pipeline
```bash
# Graceful shutdown
./scripts/stop-pipeline.sh

# Complete cleanup
./scripts/stop-pipeline.sh --volumes --clean-data
```

## Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check what's using the ports
   lsof -i :8080,8082,9092,5433,8123
   
   # Stop conflicting services or modify .env file
   ```

2. **Docker Issues**
   ```bash
   # Check Docker daemon
   docker info
   
   # Clean up Docker resources
   docker system prune -a
   ```

3. **Permission Issues**
   ```bash
   # Make scripts executable
   chmod +x scripts/*.sh
   
   # Fix data directory permissions
   sudo chown -R $USER:$USER data/
   ```

4. **Service Health Issues**
   ```bash
   # Check specific service logs
   ./scripts/logs.sh errors [service-name]
   
   # Generate health report
   ./scripts/health-check.sh report
   ```

### Debug Mode

For development and debugging:

```bash
# Start in development mode
./scripts/start-pipeline.sh --dev

# This enables:
# - Code mounting for live changes
# - Debug ports for remote debugging
# - Verbose logging
# - Development-specific configurations
```

## Environment Configuration

The scripts use configuration from:
- `.env` - Environment variables
- `deployment/docker/docker-compose.yml` - Main configuration
- `deployment/docker/docker-compose.override.yml` - Development overrides

## Data Persistence

Data is persisted in the following directories:
- `data/postgres/` - PostgreSQL data
- `data/clickhouse/` - ClickHouse data
- `data/kafka/` - Kafka logs and topics
- `data/zookeeper/` - Zookeeper data
- `data/flink/` - Flink checkpoints and savepoints
- `logs/` - Application logs and exports

## Security Considerations

- Default credentials are used for development
- Change passwords in `.env` for production
- Ensure proper network security for production deployments
- Regular backup of data directories is recommended

## Support

For issues or questions:
1. Check the logs: `./scripts/logs.sh errors`
2. Run health check: `./scripts/health-check.sh`
3. Generate health report: `./scripts/health-check.sh report`
4. Review the main project README.md for architecture details 
# 🚀 Crypto Streaming Data Pipeline

A real-time cryptocurrency data streaming and processing pipeline built with Apache Kafka, Apache Flink, and PostgreSQL. This pipeline ingests live cryptocurrency data from multiple sources, processes it in real-time, and stores it in an OLTP database for analysis and monitoring.

## 📋 Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Development](#development)
- [Monitoring](#monitoring)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │     Apache      │    │     Apache      │
│                 │───▶│     Kafka       │───▶│     Flink       │
│ • Binance WS    │    │                 │    │                 │
│ • CoinGecko API │    │ • Price Data    │    │ • Processing    │
└─────────────────┘    │ • Market Data   │    │ • Indicators    │
                       └─────────────────┘    │ • Anomalies     │
                                              └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Monitoring    │    │      OLTP       │    │   Connectors    │
│                 │    │   PostgreSQL    │◀───│                 │
│ • Metrics       │    │                 │    │ • Kafka→OLTP    │
│ • Alerts        │    │ • Price Data    │    │ • Data Sync     │
└─────────────────┘    │ • Indicators    │    └─────────────────┘
                       │ • Anomalies     │
                       └─────────────────┘
```

## ✨ Features

- **Real-time Data Ingestion**: Live cryptocurrency data from Binance WebSocket and CoinGecko API
- **Stream Processing**: Apache Flink jobs for technical indicators and anomaly detection
- **OLTP Database**: PostgreSQL for transactional data storage with optimized schema
- **Scalable Architecture**: Containerized services with Docker Compose
- **Monitoring**: Built-in health checks and metrics collection
- **Development Tools**: Pre-commit hooks, testing framework, and code quality tools

## 📋 Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- Python 3.8+ (for development)
- 8GB RAM minimum
- 20GB free disk space

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd crypto-pipeline
```

### 2. Start the Pipeline

```bash
./scripts/start-pipeline.sh
```

This will:
- Build all Docker images
- Start infrastructure services (Kafka, PostgreSQL, Flink)
- Launch data ingestion and processing components
- Set up monitoring and health checks

### 3. Verify Services

- **Kafka UI**: http://localhost:8080
- **Flink Web UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432 (admin/admin)

### 4. Stop the Pipeline

```bash
./scripts/stop-pipeline.sh
```

## 📁 Project Structure

```
crypto-pipeline/
├── src/                          # Source code
│   ├── pipeline/                # Main pipeline package
│   │   ├── ingestion/           # Data ingestion components
│   │   │   └── producer.py      # Kafka producer for crypto data
│   │   ├── processing/          # Stream processing jobs
│   │   │   └── stream_processor.py # Flink processing job
│   │   ├── connectors/          # Data connectors
│   │   │   └── kafka_to_psql.py # Kafka to PostgreSQL connector
│   │   └── services/            # Utility services
│   │       └── oltp_service.py   # Database query interface
├── infrastructure/              # Infrastructure as code
│   ├── docker/                 # Docker configurations
│   │   ├── Dockerfile.pyflink  # Flink job container
│   │   └── Dockerfile.connector # OLTP connector container
│   ├── init-scripts/           # Database initialization
│   │   └── init-oltp.sql      # PostgreSQL schema setup
│   └── docker-compose.yml     # Service orchestration
├── config/                     # Configuration files
│   ├── kafka/                 # Kafka configuration
│   ├── flink/                 # Flink configuration
│   └── postgres/              # PostgreSQL configuration
├── scripts/                   # Utility scripts
│   ├── start-pipeline.sh     # Start all services
│   ├── stop-pipeline.sh      # Stop all services
│   └── setup-dev.sh          # Development environment setup
├── tests/                    # Test suite
│   ├── unit/                # Unit tests
│   └── integration/         # Integration tests
├── docs/                    # Documentation
│   └── OLTP_README.md      # OLTP database documentation
└── README.md               # This file
```

## ⚙️ Configuration

### Environment Variables

Key configuration options (see `.env` file):

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# PostgreSQL Configuration
POSTGRES_HOST=postgres-oltp
POSTGRES_DB=crypto_oltp
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin

# Data Sources
CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,DOGEUSDT,BNBUSDT,XRPUSDT
BINANCE_WS_URL=wss://stream.binance.com:9443/ws/

# Application Settings
LOG_LEVEL=INFO
ENVIRONMENT=development
```

### Service Configuration

- **Kafka**: `config/kafka/kafka.properties`
- **Flink**: `config/flink/flink-conf.yaml`
- **PostgreSQL**: `config/postgres/postgresql.conf`

## 🛠️ Development

### Setup Development Environment

```bash
./scripts/setup-dev.sh
```

This will:
- Create Python virtual environment
- Install development dependencies
- Set up pre-commit hooks
- Create test structure
- Configure code quality tools

### Activate Virtual Environment

```bash
source venv/bin/activate
```

### Run Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src/

# Run specific test category
pytest tests/unit/
pytest tests/integration/
```

### Code Quality

```bash
# Format code
black src/

# Check code style
flake8 src/

# Type checking
mypy src/
```

### Adding New Components

1. **Data Source**: Add new producer in `src/pipeline/ingestion/`
2. **Processing Job**: Add Flink job in `src/pipeline/processing/`
3. **Connector**: Add new connector in `src/pipeline/connectors/`
4. **Service**: Add utility service in `src/pipeline/services/`

## 📊 Monitoring

### Health Checks

All services include health checks:

```bash
# Check service status
docker-compose ps

# View service logs
docker-compose logs -f [service-name]

# Check specific service health
docker-compose exec postgres-oltp pg_isready
```

### Metrics

- **Kafka**: Topic metrics, consumer lag
- **Flink**: Job metrics, throughput, latency
- **PostgreSQL**: Connection count, query performance

### Alerts

Configure alerts for:
- Service downtime
- High consumer lag
- Database connection issues
- Processing errors

## 📚 API Reference

### OLTP Database Queries

```python
from src.pipeline.services.oltp_service import OLTPService

# Initialize query interface
queries = OLTPService()

# Get latest prices
latest_prices = queries.get_latest_prices()

# Get price history
btc_history = queries.get_price_history('BTCUSDT', hours=24)

# Get technical indicators
indicators = queries.get_latest_indicators('ETHUSDT')

# Check for anomalies
anomalies = queries.get_recent_anomalies()
```

### Kafka Topics

- `crypto-prices`: Real-time price data
- `crypto-indicators`: Technical indicators
- `crypto-anomalies`: Detected anomalies

## 🔧 Troubleshooting

### Common Issues

#### Services Won't Start

```bash
# Check Docker daemon
docker info

# Check port conflicts
netstat -tulpn | grep :8080
netstat -tulpn | grep :5432

# Clean up containers
docker-compose down -v --remove-orphans
```

#### Database Connection Issues

```bash
# Check PostgreSQL logs
docker-compose logs postgres-oltp

# Test connection
docker-compose exec postgres-oltp psql -U admin -d crypto_oltp -c "SELECT 1;"
```

#### Kafka Issues

```bash
# Check Kafka logs
docker-compose logs kafka

# List topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer groups
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

#### Flink Job Issues

```bash
# Check Flink logs
docker-compose logs jobmanager taskmanager

# Access Flink Web UI
open http://localhost:8081
```

### Performance Tuning

1. **Increase Memory**: Adjust container memory limits in `docker-compose.yml`
2. **Kafka Partitions**: Increase topic partitions for higher throughput
3. **Flink Parallelism**: Adjust parallelism based on available resources
4. **Database**: Tune PostgreSQL configuration for your workload

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Run tests: `pytest tests/`
5. Run code quality checks: `black src/ && flake8 src/`
6. Commit your changes: `git commit -m 'Add amazing feature'`
7. Push to the branch: `git push origin feature/amazing-feature`
8. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guide
- Write comprehensive tests
- Update documentation
- Use meaningful commit messages
- Add type hints to new code

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Kafka for stream processing
- Apache Flink for real-time analytics
- PostgreSQL for reliable data storage
- Binance and CoinGecko for cryptocurrency data

---

**Happy Trading! 📈**
```bash
docker exec -it jobmanager flink run -py /opt/flink/jobs/stream_processor.py
```
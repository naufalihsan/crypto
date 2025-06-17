.PHONY: help install install-dev clean test test-unit test-integration lint format type-check pre-commit docker-build docker-up docker-down docker-logs setup-dev

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Installation targets
install: ## Install the package
	pip install -e .

install-dev: ## Install the package with development dependencies
	pip install -e ".[dev,monitoring,analysis]"

# Development setup
setup-dev: install-dev ## Set up development environment
	pre-commit install
	@echo "Development environment setup complete!"

# Cleaning targets
clean: ## Clean build artifacts and cache files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +

# Testing targets
test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests only
	pytest tests/ -v -m "unit"

test-integration: ## Run integration tests only
	pytest tests/ -v -m "integration"

test-coverage: ## Run tests with coverage report
	pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

# Code quality targets
lint: ## Run linting checks
	flake8 src/ tests/
	black --check src/ tests/

format: ## Format code with black
	black src/ tests/

type-check: ## Run type checking with mypy
	mypy src/

pre-commit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

# Docker targets
docker-build: ## Build all Docker images
	docker-compose -f deployment/docker/docker-compose.yml build

docker-build-streamlit: ## Build only the Streamlit dashboard image
	docker-compose -f deployment/docker/docker-compose.yml build streamlit-dashboard

docker-up: ## Start all services with Docker Compose
	docker-compose -f deployment/docker/docker-compose.yml up -d

docker-up-with-dashboard: ## Start all services including Streamlit dashboard
	docker-compose -f deployment/docker/docker-compose.yml up -d

docker-down: ## Stop all services
	docker-compose -f deployment/docker/docker-compose.yml down

docker-logs: ## Show logs from all services
	docker-compose -f deployment/docker/docker-compose.yml logs -f

docker-logs-streamlit: ## Show logs from Streamlit dashboard only
	docker-compose -f deployment/docker/docker-compose.yml logs -f streamlit-dashboard

docker-restart: docker-down docker-up ## Restart all services

# Database targets
db-init: ## Initialize databases with schema
	python tools/scripts/init_databases.py

db-migrate: ## Run database migrations
	python tools/scripts/migrate_databases.py

# Pipeline targets
start-producer: ## Start the crypto data producer
	python -m pipeline.ingestion.producer

start-oltp-connector: ## Start the OLTP connector
	python -m pipeline.connectors.kafka_to_psql

start-olap-connector: ## Start the OLAP connector
	python -m pipeline.connectors.kafka_to_clickhouse

start-flink-job: ## Start the Flink processing job
	python -m pipeline.processing.stream_processor

start-olap-service: ## Start the OLAP service
	python -m src.pipeline.services.olap_service

test-olap-service: ## Test the OLAP service endpoints
	python scripts/test_olap_service.py

# Monitoring targets
monitor-kafka: ## Monitor Kafka topics
	python tools/scripts/monitor_kafka.py

monitor-db: ## Monitor database performance
	python tools/scripts/monitor_databases.py

# Utility targets
generate-docs: ## Generate documentation
	@echo "Documentation generation not yet implemented"

validate-config: ## Validate configuration files
	python tools/scripts/validate_config.py

check-dependencies: ## Check for outdated dependencies
	pip list --outdated

# Development workflow
dev-setup: clean install-dev setup-dev ## Complete development setup
	@echo "Development environment ready!"

dev-check: lint type-check test-unit ## Run all development checks
	@echo "All checks passed!"

# Production targets
build: clean ## Build distribution packages
	python -m build

release: build ## Build and upload to PyPI (requires authentication)
	python -m twine upload dist/*

# Infrastructure targets
infra-up: ## Start infrastructure services only
	docker-compose -f deployment/docker/docker-compose.yml up -d kafka postgres clickhouse

infra-down: ## Stop infrastructure services
	docker-compose -f deployment/docker/docker-compose.yml stop kafka postgres clickhouse

# Backup targets
backup-postgres: ## Backup PostgreSQL database
	python tools/scripts/backup_postgres.py

backup-clickhouse: ## Backup ClickHouse database
	python tools/scripts/backup_clickhouse.py

# Performance targets
benchmark: ## Run performance benchmarks
	python tools/scripts/benchmark.py

profile: ## Profile application performance
	python tools/scripts/profile_app.py

# Visualization targets
start-dashboard: ## Start Streamlit dashboard locally
	streamlit run src/pipeline/visualization/streamlit_app.py --server.port=8501

dashboard-dev: ## Start Streamlit dashboard in development mode with auto-reload
	streamlit run src/pipeline/visualization/streamlit_app.py --server.port=8501 --server.runOnSave=true

test-dashboard: ## Test dashboard connectivity to services
	python -c "import requests; print('OLTP Service:', requests.get('http://localhost:8001/health').status_code); print('OLAP Service:', requests.get('http://localhost:8002/health').status_code)" 
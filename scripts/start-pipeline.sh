#!/bin/bash

# Crypto Pipeline Startup Script
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
COMPOSE_FILE="deployment/docker/docker-compose.yml"
OVERRIDE_FILE="deployment/docker/docker-compose.override.yml"
PROJECT_NAME="crypto-pipeline"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    print_status "Checking Docker daemon..."
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    print_success "Docker daemon is running"
}

# Function to create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    mkdir -p data/{postgres,clickhouse,kafka,zookeeper}
    mkdir -p logs
    print_success "Directories created"
}

# Function to start services
start_services() {
    print_status "Starting Crypto Pipeline services..."
    
    if [ "$DEV_MODE" = true ]; then
        docker-compose -f $COMPOSE_FILE -f $OVERRIDE_FILE -p $PROJECT_NAME up -d
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME up -d
    fi
    
    print_success "Services started"
}

# Function to show service URLs
show_urls() {
    print_success "Crypto Pipeline started successfully!"
    echo ""
    echo "Service URLs:"
    echo "  Kafka UI:          http://localhost:8080"
    echo "  Producer:          http://localhost:8000"
    echo "  PostgreSQL:        localhost:5432 (admin/admin)"
    echo "  ClickHouse:        http://localhost:8123 (admin/admin)"
    echo ""
    echo "To view logs: docker-compose -f $COMPOSE_FILE logs -f [service-name]"
    echo "To stop:      ./scripts/stop-pipeline.sh"
}

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dev          Use development configuration"
    echo "  --build        Force rebuild of images"
    echo "  --help         Show this help message"
    echo ""
}

# Parse command line arguments
DEV_MODE=false
FORCE_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dev)
            DEV_MODE=true
            shift
            ;;
        --build)
            FORCE_BUILD=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_status "Starting Crypto Pipeline..."
    
    check_docker
    create_directories
    
    if [ "$FORCE_BUILD" = true ]; then
        print_status "Building images..."
        if [ "$DEV_MODE" = true ]; then
            docker-compose -f $COMPOSE_FILE -f $OVERRIDE_FILE build
        else
            docker-compose -f $COMPOSE_FILE build
        fi
    fi
    
    start_services
    
    # Wait a bit for services to start
    sleep 10
    
    show_urls
}

# Run main function
main "$@" 
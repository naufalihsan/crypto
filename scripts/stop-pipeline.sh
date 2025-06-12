#!/bin/bash

# Crypto Pipeline Stop Script
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

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to stop services
stop_services() {
    print_status "Stopping Crypto Pipeline services..."
    
    if [ "$DEV_MODE" = true ]; then
        docker-compose -f $COMPOSE_FILE -f $OVERRIDE_FILE -p $PROJECT_NAME down
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down
    fi
    
    print_success "Services stopped"
}

# Function to remove volumes
remove_volumes() {
    print_status "Removing volumes..."
    
    if [ "$DEV_MODE" = true ]; then
        docker-compose -f $COMPOSE_FILE -f $OVERRIDE_FILE -p $PROJECT_NAME down -v
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME down -v
    fi
    
    print_success "Volumes removed"
}

# Function to clean up data directories
cleanup_data() {
    print_status "Cleaning up data directories..."
    
    if [ -d "data" ]; then
        print_warning "This will remove all persistent data. Are you sure? (y/N)"
        read -r response
        if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
            rm -rf data/
            print_success "Data directories cleaned"
        else
            print_status "Data directories preserved"
        fi
    else
        print_status "No data directories to clean"
    fi
}

# Function to show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dev          Use development configuration"
    echo "  --volumes      Remove volumes (WARNING: This removes all data)"
    echo "  --clean-data   Remove data directories (WARNING: This removes all data)"
    echo "  --help         Show this help message"
    echo ""
}

# Parse command line arguments
DEV_MODE=false
REMOVE_VOLUMES=false
CLEAN_DATA=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dev)
            DEV_MODE=true
            shift
            ;;
        --volumes)
            REMOVE_VOLUMES=true
            shift
            ;;
        --clean-data)
            CLEAN_DATA=true
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
    print_status "Stopping Crypto Pipeline..."
    
    if [ "$REMOVE_VOLUMES" = true ]; then
        remove_volumes
    else
        stop_services
    fi
    
    if [ "$CLEAN_DATA" = true ]; then
        cleanup_data
    fi
    
    print_success "Crypto Pipeline stopped successfully!"
}

# Run main function
main "$@" 
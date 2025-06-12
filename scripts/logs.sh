#!/bin/bash

# Crypto Pipeline Log Management Script
# This script provides easy access to service logs with filtering and monitoring

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="deployment/docker/docker-compose.yml"
OVERRIDE_FILE="deployment/docker/docker-compose.override.yml"
PROJECT_NAME="crypto-pipeline"

# Available services
SERVICES=(
    "crypto-producer"
    "oltp-connector"
    "olap-connector"
    "jobmanager"
    "taskmanager"
    "kafka"
    "schema-registry"
    "postgres-oltp"
    "clickhouse"
    "zookeeper"
    "kafka-ui"
    "prometheus"
    "grafana"
)

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

# Function to show available services
show_services() {
    echo "Available services:"
    for service in "${SERVICES[@]}"; do
        echo "  - $service"
    done
}

# Function to validate service name
validate_service() {
    local service=$1
    for valid_service in "${SERVICES[@]}"; do
        if [ "$service" = "$valid_service" ]; then
            return 0
        fi
    done
    return 1
}

# Function to show logs for a specific service
show_service_logs() {
    local service=$1
    local lines=${2:-100}
    local follow=${3:-false}
    
    if ! validate_service "$service"; then
        print_error "Invalid service: $service"
        show_services
        exit 1
    fi
    
    print_status "Showing logs for $service (last $lines lines)..."
    
    if [ "$follow" = true ]; then
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f --tail="$lines" "$service"
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" "$service"
    fi
}

# Function to show logs for all services
show_all_logs() {
    local lines=${1:-100}
    local follow=${2:-false}
    
    print_status "Showing logs for all services (last $lines lines)..."
    
    if [ "$follow" = true ]; then
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f --tail="$lines"
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines"
    fi
}

# Function to show logs with grep filter
show_filtered_logs() {
    local service=$1
    local filter=$2
    local lines=${3:-100}
    local follow=${4:-false}
    
    if [ -n "$service" ] && ! validate_service "$service"; then
        print_error "Invalid service: $service"
        show_services
        exit 1
    fi
    
    print_status "Showing filtered logs (filter: '$filter', last $lines lines)..."
    
    if [ -n "$service" ]; then
        if [ "$follow" = true ]; then
            docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f --tail="$lines" "$service" | grep --color=always "$filter"
        else
            docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" "$service" | grep --color=always "$filter"
        fi
    else
        if [ "$follow" = true ]; then
            docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f --tail="$lines" | grep --color=always "$filter"
        else
            docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" | grep --color=always "$filter"
        fi
    fi
}

# Function to show error logs only
show_error_logs() {
    local service=$1
    local lines=${2:-100}
    
    print_status "Showing error logs (last $lines lines)..."
    
    if [ -n "$service" ]; then
        if ! validate_service "$service"; then
            print_error "Invalid service: $service"
            show_services
            exit 1
        fi
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" "$service" | grep -i --color=always -E "(error|exception|failed|fatal)"
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" | grep -i --color=always -E "(error|exception|failed|fatal)"
    fi
}

# Function to export logs to file
export_logs() {
    local service=$1
    local output_file=$2
    local lines=${3:-1000}
    
    if [ -z "$output_file" ]; then
        output_file="logs/crypto-pipeline-$(date +%Y%m%d-%H%M%S).log"
    fi
    
    # Create logs directory if it doesn't exist
    mkdir -p "$(dirname "$output_file")"
    
    print_status "Exporting logs to $output_file..."
    
    if [ -n "$service" ]; then
        if ! validate_service "$service"; then
            print_error "Invalid service: $service"
            show_services
            exit 1
        fi
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" "$service" > "$output_file"
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" > "$output_file"
    fi
    
    print_success "Logs exported to $output_file"
}

# Function to show log statistics
show_log_stats() {
    local service=$1
    local lines=${2:-1000}
    
    print_status "Analyzing log statistics (last $lines lines)..."
    
    if [ -n "$service" ]; then
        if ! validate_service "$service"; then
            print_error "Invalid service: $service"
            show_services
            exit 1
        fi
        local logs=$(docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines" "$service")
    else
        local logs=$(docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail="$lines")
    fi
    
    echo "Log Statistics:"
    echo "  Total lines: $(echo "$logs" | wc -l)"
    echo "  Error lines: $(echo "$logs" | grep -i -c -E "(error|exception|failed|fatal)" || echo "0")"
    echo "  Warning lines: $(echo "$logs" | grep -i -c "warning" || echo "0")"
    echo "  Info lines: $(echo "$logs" | grep -i -c "info" || echo "0")"
    echo "  Debug lines: $(echo "$logs" | grep -i -c "debug" || echo "0")"
    
    echo ""
    echo "Top 10 most frequent log patterns:"
    echo "$logs" | sed 's/.*\] //' | sort | uniq -c | sort -nr | head -10
}

# Function to monitor logs in real-time with highlights
monitor_logs() {
    local service=$1
    
    print_status "Monitoring logs in real-time (Ctrl+C to stop)..."
    print_status "Errors in RED, Warnings in YELLOW, Info in GREEN"
    
    if [ -n "$service" ]; then
        if ! validate_service "$service"; then
            print_error "Invalid service: $service"
            show_services
            exit 1
        fi
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f "$service" | \
            sed -u -e "s/.*ERROR.*/$(printf '\033[31m')&$(printf '\033[0m')/" \
                   -e "s/.*WARN.*/$(printf '\033[33m')&$(printf '\033[0m')/" \
                   -e "s/.*INFO.*/$(printf '\033[32m')&$(printf '\033[0m')/"
    else
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs -f | \
            sed -u -e "s/.*ERROR.*/$(printf '\033[31m')&$(printf '\033[0m')/" \
                   -e "s/.*WARN.*/$(printf '\033[33m')&$(printf '\033[0m')/" \
                   -e "s/.*INFO.*/$(printf '\033[32m')&$(printf '\033[0m')/"
    fi
}

# Function to show usage
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  show [SERVICE]           Show logs for a service or all services"
    echo "  follow [SERVICE]         Follow logs in real-time"
    echo "  errors [SERVICE]         Show only error logs"
    echo "  filter PATTERN [SERVICE] Show logs matching pattern"
    echo "  export [SERVICE] [FILE]  Export logs to file"
    echo "  stats [SERVICE]          Show log statistics"
    echo "  monitor [SERVICE]        Monitor logs with color highlighting"
    echo "  services                 List available services"
    echo ""
    echo "Options:"
    echo "  -n, --lines NUM         Number of lines to show (default: 100)"
    echo "  -f, --follow           Follow logs in real-time"
    echo "  --dev                  Use development configuration"
    echo "  --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 show crypto-producer"
    echo "  $0 follow kafka"
    echo "  $0 errors"
    echo "  $0 filter \"ERROR\" crypto-producer"
    echo "  $0 export crypto-producer logs/producer.log"
    echo "  $0 stats"
    echo "  $0 monitor"
}

# Parse command line arguments
COMMAND=""
SERVICE=""
PATTERN=""
OUTPUT_FILE=""
LINES=100
FOLLOW=false
DEV_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        show|follow|errors|filter|export|stats|monitor|services)
            COMMAND=$1
            shift
            ;;
        -n|--lines)
            LINES=$2
            shift 2
            ;;
        -f|--follow)
            FOLLOW=true
            shift
            ;;
        --dev)
            DEV_MODE=true
            COMPOSE_FILE="$COMPOSE_FILE -f $OVERRIDE_FILE"
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        -*)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            if [ -z "$COMMAND" ]; then
                COMMAND="show"
                SERVICE=$1
            elif [ "$COMMAND" = "filter" ] && [ -z "$PATTERN" ]; then
                PATTERN=$1
            elif [ -z "$SERVICE" ]; then
                SERVICE=$1
            elif [ "$COMMAND" = "export" ] && [ -z "$OUTPUT_FILE" ]; then
                OUTPUT_FILE=$1
            fi
            shift
            ;;
    esac
done

# Default command
if [ -z "$COMMAND" ]; then
    COMMAND="show"
fi

# Execute command
case $COMMAND in
    show)
        if [ "$FOLLOW" = true ]; then
            show_service_logs "$SERVICE" "$LINES" true
        else
            if [ -n "$SERVICE" ]; then
                show_service_logs "$SERVICE" "$LINES" false
            else
                show_all_logs "$LINES" false
            fi
        fi
        ;;
    follow)
        if [ -n "$SERVICE" ]; then
            show_service_logs "$SERVICE" "$LINES" true
        else
            show_all_logs "$LINES" true
        fi
        ;;
    errors)
        show_error_logs "$SERVICE" "$LINES"
        ;;
    filter)
        if [ -z "$PATTERN" ]; then
            print_error "Pattern is required for filter command"
            usage
            exit 1
        fi
        show_filtered_logs "$SERVICE" "$PATTERN" "$LINES" "$FOLLOW"
        ;;
    export)
        export_logs "$SERVICE" "$OUTPUT_FILE" "$LINES"
        ;;
    stats)
        show_log_stats "$SERVICE" "$LINES"
        ;;
    monitor)
        monitor_logs "$SERVICE"
        ;;
    services)
        show_services
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        usage
 
#!/bin/bash

# Crypto Pipeline Health Check Script
# This script monitors all services and provides detailed health status

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
TIMEOUT=30

# Service configurations
declare -A SERVICE_PORTS=(
    ["zookeeper"]="2181"
    ["kafka"]="9092"
    ["schema-registry"]="8081"
    ["postgres-oltp"]="5432"
    ["clickhouse"]="8123"
    ["jobmanager"]="8081"
    ["taskmanager"]=""
    ["crypto-producer"]="8000"
    ["oltp-connector"]="8001"
    ["olap-connector"]="8002"
    ["kafka-ui"]="8080"
    ["prometheus"]="9090"
    ["grafana"]="3000"
)

declare -A SERVICE_HEALTH_URLS=(
    ["crypto-producer"]="http://localhost:8000/health"
    ["oltp-connector"]="http://localhost:8001/health"
    ["olap-connector"]="http://localhost:8002/health"
    ["kafka-ui"]="http://localhost:8080"
    ["prometheus"]="http://localhost:9090/-/healthy"
    ["grafana"]="http://localhost:3000/api/health"
    ["clickhouse"]="http://localhost:8123/ping"
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

print_healthy() {
    echo -e "${GREEN}✓${NC} $1"
}

print_unhealthy() {
    echo -e "${RED}✗${NC} $1"
}

print_unknown() {
    echo -e "${YELLOW}?${NC} $1"
}

# Function to check if a port is open
check_port() {
    local host=$1
    local port=$2
    local timeout=${3:-5}
    
    if command -v nc >/dev/null 2>&1; then
        nc -z -w$timeout "$host" "$port" >/dev/null 2>&1
    elif command -v telnet >/dev/null 2>&1; then
        timeout $timeout bash -c "echo >/dev/tcp/$host/$port" >/dev/null 2>&1
    else
        return 1
    fi
}

# Function to check HTTP endpoint
check_http() {
    local url=$1
    local timeout=${2:-10}
    
    if command -v curl >/dev/null 2>&1; then
        curl -s --max-time $timeout "$url" >/dev/null 2>&1
    elif command -v wget >/dev/null 2>&1; then
        wget -q --timeout=$timeout --tries=1 -O /dev/null "$url" >/dev/null 2>&1
    else
        return 1
    fi
}

# Function to get container status
get_container_status() {
    local service=$1
    local container_id=$(docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps -q "$service" 2>/dev/null)
    
    if [ -z "$container_id" ]; then
        echo "not_found"
        return
    fi
    
    local status=$(docker inspect --format='{{.State.Status}}' "$container_id" 2>/dev/null)
    echo "$status"
}

# Function to get container health
get_container_health() {
    local service=$1
    local container_id=$(docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps -q "$service" 2>/dev/null)
    
    if [ -z "$container_id" ]; then
        echo "unknown"
        return
    fi
    
    local health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no_healthcheck{{end}}' "$container_id" 2>/dev/null)
    echo "$health"
}

# Function to check individual service
check_service() {
    local service=$1
    local status=$(get_container_status "$service")
    local health=$(get_container_health "$service")
    local port=${SERVICE_PORTS[$service]}
    local health_url=${SERVICE_HEALTH_URLS[$service]}
    
    printf "%-20s" "$service"
    
    # Check container status
    case $status in
        "running")
            printf "${GREEN}%-12s${NC}" "Running"
            ;;
        "exited")
            printf "${RED}%-12s${NC}" "Exited"
            ;;
        "restarting")
            printf "${YELLOW}%-12s${NC}" "Restarting"
            ;;
        "not_found")
            printf "${RED}%-12s${NC}" "Not Found"
            ;;
        *)
            printf "${YELLOW}%-12s${NC}" "$status"
            ;;
    esac
    
    # Check health status
    case $health in
        "healthy")
            printf "${GREEN}%-12s${NC}" "Healthy"
            ;;
        "unhealthy")
            printf "${RED}%-12s${NC}" "Unhealthy"
            ;;
        "starting")
            printf "${YELLOW}%-12s${NC}" "Starting"
            ;;
        "no_healthcheck")
            printf "${BLUE}%-12s${NC}" "No Check"
            ;;
        *)
            printf "${YELLOW}%-12s${NC}" "$health"
            ;;
    esac
    
    # Check port connectivity
    if [ -n "$port" ] && [ "$status" = "running" ]; then
        if check_port "localhost" "$port" 3; then
            printf "${GREEN}%-12s${NC}" "Port OK"
        else
            printf "${RED}%-12s${NC}" "Port Fail"
        fi
    else
        printf "%-12s" "N/A"
    fi
    
    # Check HTTP health endpoint
    if [ -n "$health_url" ] && [ "$status" = "running" ]; then
        if check_http "$health_url" 5; then
            printf "${GREEN}%-12s${NC}" "HTTP OK"
        else
            printf "${RED}%-12s${NC}" "HTTP Fail"
        fi
    else
        printf "%-12s" "N/A"
    fi
    
    echo ""
}

# Function to check all services
check_all_services() {
    echo "Crypto Pipeline Health Check"
    echo "============================"
    echo ""
    printf "%-20s %-12s %-12s %-12s %-12s\n" "Service" "Status" "Health" "Port" "HTTP"
    printf "%-20s %-12s %-12s %-12s %-12s\n" "-------" "------" "------" "----" "----"
    
    local services=(
        "zookeeper"
        "kafka"
        "schema-registry"
        "postgres-oltp"
        "clickhouse"
        "jobmanager"
        "taskmanager"
        "crypto-producer"
        "oltp-connector"
        "olap-connector"
        "kafka-ui"
        "prometheus"
        "grafana"
    )
    
    local healthy_count=0
    local total_count=${#services[@]}
    
    for service in "${services[@]}"; do
        check_service "$service"
        
        # Count healthy services
        local status=$(get_container_status "$service")
        local health=$(get_container_health "$service")
        
        if [ "$status" = "running" ] && ([ "$health" = "healthy" ] || [ "$health" = "no_healthcheck" ]); then
            ((healthy_count++))
        fi
    done
    
    echo ""
    echo "Summary: $healthy_count/$total_count services healthy"
    
    if [ $healthy_count -eq $total_count ]; then
        print_success "All services are healthy!"
        return 0
    else
        print_warning "Some services are not healthy"
        return 1
    fi
}

# Function to check specific service dependencies
check_dependencies() {
    print_status "Checking service dependencies..."
    
    local issues=0
    
    # Check Kafka dependencies
    if ! check_port "localhost" "2181" 3; then
        print_unhealthy "Zookeeper (required for Kafka)"
        ((issues++))
    else
        print_healthy "Zookeeper"
    fi
    
    if ! check_port "localhost" "9092" 3; then
        print_unhealthy "Kafka (required for data streaming)"
        ((issues++))
    else
        print_healthy "Kafka"
    fi
    
    # Check Database dependencies
    if ! check_port "localhost" "5432" 3; then
        print_unhealthy "PostgreSQL OLTP (required for transactional data)"
        ((issues++))
    else
        print_healthy "PostgreSQL OLTP"
    fi
    
    if ! check_port "localhost" "8123" 3; then
        print_unhealthy "ClickHouse (required for analytical data)"
        ((issues++))
    else
        print_healthy "ClickHouse"
    fi
    
    # Check Flink dependencies
    if ! check_port "localhost" "8081" 3; then
        print_unhealthy "Flink JobManager (required for stream processing)"
        ((issues++))
    else
        print_healthy "Flink JobManager"
    fi
    
    echo ""
    if [ $issues -eq 0 ]; then
        print_success "All dependencies are healthy"
        return 0
    else
        print_error "$issues dependency issues found"
        return 1
    fi
}

# Function to check data flow
check_data_flow() {
    print_status "Checking data flow..."
    
    # Check if producer is producing data
    if check_http "http://localhost:8000/health" 5; then
        print_healthy "Producer service is responding"
    else
        print_unhealthy "Producer service is not responding"
        return 1
    fi
    
    # Check if connectors are healthy
    if check_http "http://localhost:8001/health" 5; then
        print_healthy "OLTP Connector is responding"
    else
        print_unhealthy "OLTP Connector is not responding"
    fi
    
    if check_http "http://localhost:8002/health" 5; then
        print_healthy "OLAP Connector is responding"
    else
        print_unhealthy "OLAP Connector is not responding"
    fi
    
    # TODO: Add actual data flow checks (e.g., check if data is flowing through Kafka topics)
    print_status "Data flow validation requires additional implementation"
}

# Function to check resource usage
check_resources() {
    print_status "Checking resource usage..."
    
    # Get container resource usage
    local containers=$(docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps -q 2>/dev/null)
    
    if [ -z "$containers" ]; then
        print_warning "No containers found"
        return 1
    fi
    
    echo "Container Resource Usage:"
    printf "%-20s %-10s %-10s %-15s\n" "Container" "CPU %" "Memory" "Status"
    printf "%-20s %-10s %-10s %-15s\n" "---------" "-----" "------" "------"
    
    for container in $containers; do
        local name=$(docker inspect --format='{{.Name}}' "$container" | sed 's/^.//')
        local stats=$(docker stats --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}" "$container" 2>/dev/null | tail -n 1)
        
        if [ -n "$stats" ]; then
            local cpu=$(echo "$stats" | awk '{print $1}')
            local memory=$(echo "$stats" | awk '{print $2}')
            printf "%-20s %-10s %-10s %-15s\n" "$name" "$cpu" "$memory" "Running"
        fi
    done
    
    echo ""
}

# Function to run continuous monitoring
monitor_health() {
    local interval=${1:-30}
    
    print_status "Starting continuous health monitoring (interval: ${interval}s)"
    print_status "Press Ctrl+C to stop"
    
    while true; do
        clear
        echo "$(date): Crypto Pipeline Health Monitor"
        echo "======================================"
        echo ""
        
        check_all_services
        echo ""
        check_dependencies
        echo ""
        
        sleep "$interval"
    done
}

# Function to generate health report
generate_report() {
    local output_file=${1:-"health-report-$(date +%Y%m%d-%H%M%S).txt"}
    
    print_status "Generating health report: $output_file"
    
    {
        echo "Crypto Pipeline Health Report"
        echo "Generated: $(date)"
        echo "=============================="
        echo ""
        
        check_all_services
        echo ""
        check_dependencies
        echo ""
        check_resources
        echo ""
        
        echo "Docker Compose Status:"
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME ps
        echo ""
        
        echo "Recent Errors (last 100 lines):"
        docker-compose -f $COMPOSE_FILE -p $PROJECT_NAME logs --tail=100 | grep -i error || echo "No errors found"
        
    } > "$output_file"
    
    print_success "Health report saved to: $output_file"
}

# Function to show usage
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  check              Check all services (default)"
    echo "  deps               Check service dependencies"
    echo "  flow               Check data flow"
    echo "  resources          Check resource usage"
    echo "  monitor [INTERVAL] Monitor health continuously"
    echo "  report [FILE]      Generate health report"
    echo ""
    echo "Options:"
    echo "  --dev              Use development configuration"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                 # Check all services"
    echo "  $0 deps            # Check dependencies only"
    echo "  $0 monitor 60      # Monitor every 60 seconds"
    echo "  $0 report          # Generate health report"
}

# Parse command line arguments
COMMAND="check"
INTERVAL=30
OUTPUT_FILE=""
DEV_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        check|deps|flow|resources|monitor|report)
            COMMAND=$1
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
            if [ "$COMMAND" = "monitor" ] && [[ $1 =~ ^[0-9]+$ ]]; then
                INTERVAL=$1
            elif [ "$COMMAND" = "report" ]; then
                OUTPUT_FILE=$1
            fi
            shift
            ;;
    esac
done

# Execute command
case $COMMAND in
    check)
        check_all_services
        ;;
    deps)
        check_dependencies
        ;;
    flow)
        check_data_flow
        ;;
    resources)
        check_resources
        ;;
    monitor)
        monitor_health "$INTERVAL"
        ;;
    report)
        generate_report "$OUTPUT_FILE"
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac 
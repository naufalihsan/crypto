#!/bin/bash

# Start JobManager and submit PyFlink job
set -e

echo "Starting Flink JobManager with automatic job submission..."

# Start JobManager in background
echo "Starting JobManager..."
/docker-entrypoint.sh jobmanager &
JOBMANAGER_PID=$!

# Function to check if JobManager is ready
check_jobmanager() {
    curl -f -s "http://localhost:8081/overview" > /dev/null 2>&1
    return $?
}

# Function to check if TaskManagers are available
check_taskmanagers() {
    local available_slots=$(curl -s "http://localhost:8081/overview" 2>/dev/null | grep -o '"slots-available":[0-9]*' | cut -d':' -f2)
    if [ -n "$available_slots" ] && [ "$available_slots" -gt 0 ]; then
        echo "Found $available_slots available TaskManager slots"
        return 0
    else
        echo "No TaskManager slots available yet"
        return 1
    fi
}

# Function to submit the job
submit_job() {
    echo "Submitting PyFlink job..."
    flink run -py /opt/flink/jobs/flink_stream_processor.py
    return $?
}

# Wait for JobManager to be ready
echo "Waiting for JobManager to be ready..."
retry_count=0
max_retries=30

while [ $retry_count -lt $max_retries ]; do
    if check_jobmanager; then
        echo "JobManager is ready!"
        break
    else
        retry_count=$((retry_count + 1))
        echo "JobManager not ready. Attempt $retry_count/$max_retries. Waiting 10s..."
        sleep 10
    fi
done

if [ $retry_count -eq $max_retries ]; then
    echo "ERROR: JobManager not available after $max_retries attempts"
    kill $JOBMANAGER_PID 2>/dev/null || true
    exit 1
fi

# Wait for TaskManagers to be ready
echo "Waiting for TaskManagers to be ready..."
retry_count=0
max_retries=30

while [ $retry_count -lt $max_retries ]; do
    if check_taskmanagers; then
        echo "TaskManagers are ready!"
        break
    else
        retry_count=$((retry_count + 1))
        echo "TaskManagers not ready. Attempt $retry_count/$max_retries. Waiting 10s..."
        sleep 10
    fi
done

if [ $retry_count -eq $max_retries ]; then
    echo "WARNING: No TaskManagers available after $max_retries attempts. Job submission may fail."
fi

# Submit the job
echo "JobManager and TaskManagers are ready. Submitting job..."
if submit_job; then
    echo "Job submitted successfully!"
else
    echo "WARNING: Job submission failed, but JobManager will continue running"
fi

# Wait for JobManager process
echo "JobManager running with job. Waiting for process..."
wait $JOBMANAGER_PID 
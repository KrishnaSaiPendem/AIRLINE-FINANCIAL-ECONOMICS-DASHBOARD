#!/bin/bash

# Set error handling
set -e

# Configuration
LOG_FILE="/home/ec2-user/s3_upload.log"
BUCKET="airline-dashboard-project-data"
FORM41_PATH="automation/form41/"
STOCK_PATH="automation/stockdata/"

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Function to ensure S3 directory exists by creating a placeholder
ensure_s3_directory() {
    local path=$1
    log_message "Ensuring directory exists: s3://${BUCKET}/${path}"
    
    # Create an empty placeholder object to ensure directory exists
    if ! aws s3api head-object --bucket "$BUCKET" --key "${path}.placeholder" --profile daen690 2>/dev/null; then
        log_message "Creating directory placeholder..."
        echo "" | aws s3 cp - "s3://${BUCKET}/${path}.placeholder" --profile daen690
    fi
}

# Function to upload files
upload_to_s3() {
    log_message "Starting S3 upload process..."

    # Ensure directories exist
    ensure_s3_directory "$FORM41_PATH"
    ensure_s3_directory "$STOCK_PATH"

    # Upload form41 files
    log_message "Uploading files from downloads directory..."
    if aws s3 cp /home/ec2-user/automation/downloads/ \
        "s3://${BUCKET}/${FORM41_PATH}" \
        --recursive \
        --profile daen690; then
        log_message "Successfully uploaded form41 files"
    else
        log_message "ERROR: Failed to upload form41 files"
        return 1
    fi

    # Upload stock data
    log_message "Uploading stock data file..."
    if aws s3 cp /home/ec2-user/automation/data/airline_stock_data.csv \
        "s3://${BUCKET}/${STOCK_PATH}" \
        --profile daen690; then
        log_message "Successfully uploaded stock data file"
    else
        log_message "ERROR: Failed to upload stock data file"
        return 1
    fi

    log_message "All uploads completed successfully"
    return 0
}

# Main execution
main() {
    upload_to_s3
}

main

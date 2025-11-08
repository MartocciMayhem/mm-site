#!/bin/bash

# Variables
INSTANCE_NAME="mm-web-db"
PROJECT_ID="martocci-mayhem"

echo "Testing alerts for $INSTANCE_NAME..."

# 1. Get current metrics as baseline
echo "Getting baseline metrics..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '{
    instance_type: .settings.tier,
    disk_size: .settings.dataDiskSizeGb,
    availability_type: .settings.availabilityType
  }'

# 2. Temporarily increase CPU and memory to trigger alert
echo "Triggering CPU alert by increasing instance size..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --tier=db-custom-4-8192

# Wait for 2 minutes
echo "Waiting for changes to take effect (2 minutes)..."
sleep 120

# 3. Check metrics after change
echo "Checking metrics after change..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '{
    instance_type: .settings.tier,
    disk_size: .settings.dataDiskSizeGb,
    availability_type: .settings.availabilityType
  }'

# 4. Reset back to original configuration
echo "Resetting instance to original configuration..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --tier=db-custom-2-4096

echo "Alert test completed. Please check the Google Cloud Console Monitoring page:"
echo "https://console.cloud.google.com/monitoring/alerting"
echo "You should receive email notifications if any thresholds were exceeded"
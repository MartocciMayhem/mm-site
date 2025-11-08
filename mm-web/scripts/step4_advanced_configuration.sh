#!/bin/bash

# Step 4: Advanced Database Configuration
echo "Implementing advanced configurations for mm-web-db..."

# Variables
INSTANCE_NAME="mm-web-db"
PROJECT_ID="martocci-mayhem"

# 1. Configure maintenance window (setting to Sunday 2 AM)
echo "Configuring maintenance window..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --maintenance-window-day=SUN \
  --maintenance-window-hour=2

# 2. Upgrade to regional availability
echo "Upgrading to regional high availability..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --availability-type=REGIONAL

# 3. Set up monitoring alerts
echo "Setting up Cloud Monitoring alerts..."

# Create CPU usage alert
gcloud beta monitoring alerts policies create \
  --display-name="mm-web-db CPU Usage Alert" \
  --project=$PROJECT_ID \
  --condition-filter="resource.type = \"cloud_sql_database\" AND resource.labels.database_id = \"$PROJECT_ID:$INSTANCE_NAME\" AND metric.type = \"cloudsql.googleapis.com/database/cpu/utilization\"" \
  --condition-threshold-value=0.8 \
  --condition-threshold-duration=300s \
  --notification-channels="email=$PROJECT_ID@gmail.com" \
  --documentation-content="CPU usage exceeded 80% for 5 minutes"

# Create storage usage alert
gcloud beta monitoring alerts policies create \
  --display-name="mm-web-db Storage Usage Alert" \
  --project=$PROJECT_ID \
  --condition-filter="resource.type = \"cloud_sql_database\" AND resource.labels.database_id = \"$PROJECT_ID:$INSTANCE_NAME\" AND metric.type = \"cloudsql.googleapis.com/database/disk/bytes_used\"" \
  --condition-threshold-value=85 \
  --condition-threshold-duration=300s \
  --notification-channels="email=$PROJECT_ID@gmail.com" \
  --documentation-content="Storage usage exceeded 85% for 5 minutes"

# Enable query insights
echo "Enabling query insights..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --query-insights-enabled

# Verify changes
echo "Verifying configurations..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '{
    maintenanceWindow: .settings.maintenanceWindow,
    availabilityType: .settings.availabilityType,
    queryInsightsEnabled: .settings.insightsConfig.queryInsightsEnabled
  }'

echo "Advanced configuration completed. Please verify:"
echo "1. Maintenance window is set to Sunday 2 AM"
echo "2. High availability is set to REGIONAL"
echo "3. Monitoring alerts are created for CPU and storage usage"
echo "4. Query insights are enabled"

# Note: Database upgrade should be planned separately as it requires downtime
echo -e "\nNOTE: PostgreSQL version upgrade (15 -> 16) should be planned separately"
echo "as it requires downtime and careful testing. Would you like to plan this upgrade next?"
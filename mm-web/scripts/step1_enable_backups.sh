#!/bin/bash

# Step 1: Enable Automated Backups and Point-in-time Recovery
echo "Configuring backup settings for mm-web-db..."

# Variables
INSTANCE_NAME="mm-web-db"
PROJECT_ID="martocci-mayhem"
BACKUP_START_TIME="02:00"  # 2 AM

# Enable automated backups and point-in-time recovery
echo "Enabling automated backups and point-in-time recovery..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --backup \
  --backup-start-time=$BACKUP_START_TIME

# Verify the changes
echo "Verifying configuration..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '.settings.backupConfiguration'

echo "Backup configuration completed. Please verify the output above shows:"
echo "1. enabled: true"
echo "2. startTime: 02:00"
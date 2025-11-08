#!/bin/bash

# Step 2: Enhance Security Configuration
echo "Configuring security settings for mm-web-db..."

# Variables
INSTANCE_NAME="mm-web-db"
PROJECT_ID="martocci-mayhem"

# First, let's verify we can access the instance
echo "Verifying instance access..."
gcloud sql instances describe $INSTANCE_NAME --project=$PROJECT_ID || exit 1

# Apply security hardening configurations
echo "Applying security configurations..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --deletion-protection \
  --require-ssl \
  --ssl-mode="TRUSTED_CLIENT_CERTIFICATE_REQUIRED" \
  --connector-enforcement="REQUIRED"

# Verify the changes
echo "Verifying security configuration..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '{
    deletionProtection: .settings.deletionProtectionEnabled,
    sslConfig: .settings.ipConfiguration.requireSsl,
    sslMode: .settings.ipConfiguration.sslMode,
    connectorEnforcement: .settings.connectorEnforcement
  }'

echo "Security configuration completed. Please verify the output above shows:"
echo "1. deletionProtection: true"
echo "2. sslConfig: true"
echo "3. sslMode: TRUSTED_CLIENT_CERTIFICATE_REQUIRED"
echo "4. connectorEnforcement: REQUIRED"
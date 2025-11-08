#!/bin/bash

# Audit script to check database configuration
echo "Auditing mm-web-db configuration..."

# Variables
INSTANCE_NAME="mm-web-db"
PROJECT_ID="martocci-mayhem"

# Check instance configuration
echo "Fetching instance configuration..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '{
    // Basic settings
    name: .name,
    databaseVersion: .databaseVersion,
    region: .region,
    tier: .settings.tier,
    
    // Backup configuration
    backupConfiguration: .settings.backupConfiguration,
    
    // Security settings
    requireSsl: .settings.ipConfiguration.requireSsl,
    databaseFlags: .settings.databaseFlags,
    passwordPolicyConfig: .settings.passwordPolicyConfig,
    deletionProtection: .deletionProtection,
    
    // Maintenance settings
    maintenanceWindow: .settings.maintenanceWindow,
    
    // Network settings
    authorizedNetworks: .settings.ipConfiguration.authorizedNetworks,
    
    // High availability settings
    availabilityType: .settings.availabilityType
  }'

echo "Configuration audit complete. Please verify:"
echo "1. Backup configuration is enabled with appropriate retention"
echo "2. SSL is required"
echo "3. Database flags are set for security"
echo "4. Password policy is properly configured"
echo "5. Deletion protection is enabled"
echo "6. Maintenance window is configured"
echo "7. Authorized networks are properly restricted"
echo "8. High availability configuration is appropriate for your needs"
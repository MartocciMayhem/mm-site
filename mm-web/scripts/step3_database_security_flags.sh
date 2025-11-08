#!/bin/bash

# Step 3: Configure Database Security Flags and Password Policies
echo "Configuring database security flags and password policies for mm-web-db..."

# Variables
INSTANCE_NAME="mm-web-db"
PROJECT_ID="martocci-mayhem"

# First, let's verify we can access the instance
echo "Verifying instance access..."
gcloud sql instances describe $INSTANCE_NAME --project=$PROJECT_ID || exit 1

# Apply database flags and password policies
echo "Applying database security flags and password policies..."
gcloud sql instances patch $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --database-flags="log_checkpoints=on,log_connections=on,log_disconnections=on,log_lock_waits=on,log_temp_files=0,ssl=on,password_encryption=scram-sha-256" \
  --enable-password-policy \
  --password-policy-min-length=12 \
  --password-policy-complexity="COMPLEXITY_DEFAULT" \
  --password-policy-reuse-interval=365 \
  --password-policy-password-change-interval=90 \
  --password-policy-disallow-username-substring

# Verify the changes
echo "Verifying database flags and password policies..."
gcloud sql instances describe $INSTANCE_NAME \
  --project=$PROJECT_ID \
  --format="json" | jq '{
    databaseFlags: .settings.databaseFlags,
    passwordPolicyEnabled: .settings.passwordPolicyConfig.enablePasswordPolicy,
    passwordPolicyMinLength: .settings.passwordPolicyConfig.minLength,
    passwordPolicyComplexity: .settings.passwordPolicyConfig.complexity,
    passwordPolicyReuseInterval: .settings.passwordPolicyConfig.reuseInterval,
    passwordPolicyChangeInterval: .settings.passwordPolicyConfig.passwordChangeInterval,
    passwordPolicyDisallowUsername: .settings.passwordPolicyConfig.disallowUsernameSubstring
  }'

echo "Database security configuration completed. Please verify the output above shows:"
echo "1. Database flags are set for enhanced logging and security"
echo "2. Password policy is enabled"
echo "3. Minimum password length is 12 characters"
echo "4. Password complexity is enforced"
echo "5. Password reuse interval is 365 days"
echo "6. Password change interval is 90 days"
echo "7. Username in password is disallowed"
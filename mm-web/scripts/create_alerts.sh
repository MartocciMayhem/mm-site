#!/bin/bash

# Variables
PROJECT_ID="martocci-mayhem"
INSTANCE_NAME="mm-web-db"

# Create CPU Alert
echo "Creating CPU Usage Alert..."
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://monitoring.googleapis.com/v3/projects/$PROJECT_ID/alertPolicies" \
  -d @- << 'EOF'
{
  "displayName": "mm-web-db CPU Usage Alert",
  "documentation": {
    "content": "CPU usage has exceeded 80% for 5 minutes",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "CPU Usage > 80%",
      "conditionThreshold": {
        "filter": "metric.type=\"cloudsql.googleapis.com/database/cpu/utilization\" AND resource.type=\"cloud_sql_database\" AND resource.labels.database_id=\"martocci-mayhem:mm-web-db\"",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_MEAN"
          }
        ],
        "comparison": "COMPARISON_GT",
        "duration": "300s",
        "thresholdValue": 0.8
      }
    }
  ],
  "combiner": "AND",
  "enabled": true
}
EOF

# Create Storage Alert
echo "Creating Storage Usage Alert..."
curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://monitoring.googleapis.com/v3/projects/$PROJECT_ID/alertPolicies" \
  -d @- << 'EOF'
{
  "displayName": "mm-web-db Storage Usage Alert",
  "documentation": {
    "content": "Storage usage has exceeded 85%",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "Storage Usage > 85%",
      "conditionThreshold": {
        "filter": "metric.type=\"cloudsql.googleapis.com/database/disk/bytes_used\" AND resource.type=\"cloud_sql_database\" AND resource.labels.database_id=\"martocci-mayhem:mm-web-db\"",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_MEAN"
          }
        ],
        "comparison": "COMPARISON_GT",
        "duration": "300s",
        "thresholdValue": 85
      }
    }
  ],
  "combiner": "AND",
  "enabled": true
}
EOF

echo "Alert policies creation completed. Please verify in the Google Cloud Console:"
echo "https://console.cloud.google.com/monitoring/alerting"
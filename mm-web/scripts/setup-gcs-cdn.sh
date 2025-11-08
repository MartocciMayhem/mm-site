#!/bin/bash
# Setup Google Cloud CDN for videos.martoccimayhem.com
# This replaces GitHub Pages with GCS + Cloud CDN

set -e

PROJECT_ID="martocci-mayhem"
BUCKET_NAME="martocci-mayhem-videos"
DOMAIN="videos.martoccimayhem.com"

echo "🚀 Setting up Cloud CDN for $DOMAIN"
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo ""

# Set project
gcloud config set project $PROJECT_ID

# 1. Create backend bucket
echo "📦 Creating backend bucket..."
gcloud compute backend-buckets create mm-videos-backend \
    --gcs-bucket-name=$BUCKET_NAME \
    --enable-cdn \
    --cache-mode=CACHE_ALL_STATIC \
    || echo "Backend bucket already exists, continuing..."

# 2. Create URL map
echo "🗺️  Creating URL map..."
gcloud compute url-maps create mm-videos-urlmap \
    --default-backend-bucket=mm-videos-backend \
    || echo "URL map already exists, continuing..."

# 3. Create managed SSL certificate
echo "🔒 Creating SSL certificate for $DOMAIN..."
gcloud compute ssl-certificates create mm-videos-cert \
    --domains=$DOMAIN \
    --global \
    || echo "SSL certificate already exists, continuing..."

# 4. Create HTTPS proxy
echo "🔌 Creating HTTPS proxy..."
gcloud compute target-https-proxies create mm-videos-proxy \
    --url-map=mm-videos-urlmap \
    --ssl-certificates=mm-videos-cert \
    || echo "HTTPS proxy already exists, continuing..."

# 5. Create global forwarding rule (gets IP address)
echo "🌍 Creating global forwarding rule..."
gcloud compute forwarding-rules create mm-videos-https-rule \
    --global \
    --target-https-proxy=mm-videos-proxy \
    --ports=443 \
    || echo "Forwarding rule already exists, continuing..."

# 6. Get the IP address
echo ""
echo "✅ Setup complete!"
echo ""
echo "📝 DNS Configuration Required:"
echo "================================"
IP=$(gcloud compute forwarding-rules describe mm-videos-https-rule --global --format="get(IPAddress)")
echo "1. Go to your DNS provider (Cloudflare/GoDaddy/etc)"
echo "2. Delete the CNAME record for 'videos' pointing to martoccimayhem.github.io"
echo "3. Create an A record:"
echo "   Name: videos"
echo "   Type: A"
echo "   Value: $IP"
echo "   TTL: 300 (5 minutes)"
echo ""
echo "⏱️  SSL Certificate Provisioning:"
echo "================================"
echo "After DNS is updated, SSL provisioning takes 15-60 minutes."
echo "Check status with:"
echo "  gcloud compute ssl-certificates describe mm-videos-cert --global"
echo ""
echo "🧪 Testing:"
echo "================================"
echo "Once DNS propagates, test:"
echo "  https://videos.martoccimayhem.com/martocci-mayhem/"
echo "  https://videos.martoccimayhem.com/savage-grandma/"
echo ""

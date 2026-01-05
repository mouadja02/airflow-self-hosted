#!/bin/bash
# One-time script to fix file ownership from UID 50000 to your user
# Run this on your Raspberry Pi BEFORE the next deployment

set -e

PROJECT_PATH="${1:-$HOME/dataops/airflow-self-hosted}"

echo "🔧 Fixing file ownership in: $PROJECT_PATH"
echo "   Changing from UID 50000 to $(whoami) (UID $(id -u))"
echo ""

# Stop containers first
echo "🛑 Stopping Docker containers..."
cd "$PROJECT_PATH"
docker compose down

# Fix ownership
echo "📝 Changing ownership to $(whoami):$(whoami)..."
sudo chown -R $(whoami):$(whoami) "$PROJECT_PATH"

# Configure git safe directory
echo "🔐 Configuring git safe directory..."
git config --global --add safe.directory "$PROJECT_PATH"

# Verify
echo ""
echo "✅ Ownership fixed!"
echo ""
echo "📊 Current ownership:"
ls -la "$PROJECT_PATH" | head -10

echo ""
echo "✅ Setup complete! Now:"
echo "   1. The files are owned by you ($(whoami))"
echo "   2. Docker will run as your user (UID $(id -u))"
echo "   3. No more permission conflicts!"
echo ""
echo "🚀 Starting containers with new configuration..."
docker compose up -d

echo ""
echo "✅ All done! Check container status with: docker compose ps"


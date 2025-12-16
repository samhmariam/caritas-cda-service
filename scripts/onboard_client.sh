#!/bin/bash
# Client onboarding automation script

set -e

if [ -z "$1" ]; then
    echo "Usage: ./onboard_client.sh <client_name>"
    echo "Example: ./onboard_client.sh stellar_bank"
    exit 1
fi

CLIENT_NAME=$1
CLIENT_DIR="clients/$CLIENT_NAME"

echo "🚀 Onboarding new client: $CLIENT_NAME"

# Check if client already exists
if [ -d "$CLIENT_DIR" ]; then
    echo "❌ Error: Client $CLIENT_NAME already exists"
    exit 1
fi

# Create client directory from template
echo "📁 Creating client directory..."
cp -r clients/_template "$CLIENT_DIR"

# Update config files with client name
echo "⚙️  Updating configuration files..."
find "$CLIENT_DIR" -type f -name "*.yml" -exec sed -i "s/_template/$CLIENT_NAME/g" {} \;
find "$CLIENT_DIR" -type f -name "*.yml" -exec sed -i "s/CLIENT/${CLIENT_NAME^^}/g" {} \;

echo ""
echo "✅ Client directory created: $CLIENT_DIR"
echo ""
echo "📝 Next steps:"
echo "1. Edit $CLIENT_DIR/config/dev.yml with client-specific values"
echo "2. Update airbyte sources list"
echo "3. Run terraform:"
echo "   cd terraform/environments/dev"
echo "   export TF_VAR_client_name=$CLIENT_NAME"
echo "   terraform plan"
echo "   terraform apply"
echo ""
echo "4. Configure Airbyte connections"
echo "5. Run initial dbt build:"
echo "   export DBT_CLIENT=$CLIENT_NAME"
echo "   cd dbt && dbt build"
echo ""
echo "See docs/runbooks/onboard_new_client.md for full checklist"

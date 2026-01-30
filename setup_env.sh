#!/bin/bash
# Environment setup script for Airflow Clinic Sync DAG
# Source this file before running Airflow: source setup_env.sh

# Airflow Configuration
export AIRFLOW_HOME="$(pwd)/airflow_home"

# Google Cloud Service Account for Google Sheets API
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/airflow_home/credentials/service-account.json"

# Google Places API Key (Required for enrichment)
# Get this from: Google Cloud Console > APIs & Services > Credentials > Create API Key
# Then enable "Places API" in the project
export GOOGLE_PLACES_API_KEY="${GOOGLE_PLACES_API_KEY:-YOUR_PLACES_API_KEY_HERE}"

# Supabase Database Configuration (READ FROM .env FILE - DO NOT HARDCODE HERE!)
# Load from .env file if it exists
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
    echo "✓ Loaded credentials from .env file"
else
    echo "WARNING: .env file not found. Supabase credentials not loaded."
    echo "Please create a .env file with SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY"
fi

# macOS fork safety (needed for Airflow scheduler on macOS)
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

echo "Environment variables set for Airflow"
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS"
echo ""
echo "NOTE: Make sure to set GOOGLE_PLACES_API_KEY before running the DAG with enrichment enabled"

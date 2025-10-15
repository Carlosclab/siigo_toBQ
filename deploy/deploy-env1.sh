#!/bin/bash
set -e

PROJECT_ID="powerbi-445616"
REGION="us-central1"
FUNCTION_NAME="siigo-etl-env1"

echo "🚀 Deploying $FUNCTION_NAME..."

gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=run_etl_pipeline \
  --trigger-http \
  --allow-unauthenticated \
  --timeout=540s \
  --memory=512MB \
  --max-instances=1 \
  --env-vars-file=config/env1.yaml \
  --project=$PROJECT_ID

echo "✅ Deployed successfully"
gcloud functions describe $FUNCTION_NAME --region=$REGION --project=$PROJECT_ID --format="value(serviceConfig.uri)"

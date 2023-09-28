# Setup

```sh
PROJECT="my-google-cloud-project"
BUCKET_EU="my-us-gcs-bucket"
BUCKET_EU="my-eu-gcs-bucket"
SERVICE_ACCOUNT="my-dataflow-worker-service-account@developer.gserviceaccount.com"

MAX_EE_REQUESTS=20
REGION_US="us-central1"
REGION_EU="eu-north1"
```

## California

```sh
python create-dataset.py \
  "california" \
 "gs://$BUCKET_US/air-quality/data" \
  --max-ee-requests=$MAX_EE_REQUESTS \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$REGION_US" \
  --temp_location="gs://$BUCKET_US/temp" \
  --service_account_email="112365280327-compute@developer.gserviceaccount.com"
```

## Houston

```sh
python create-dataset.py \
  "houston" \
  "gs://$BUCKET_US/air-quality/data" \
  --max-ee-requests=$MAX_EE_REQUESTS \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$REGION_US" \
  --temp_location="gs://$BUCKET_US/temp" \
  --service_account_email="112365280327-compute@developer.gserviceaccount.com"
```

## Amsterdam

```sh
python create-dataset.py \
  "amsterdam" \
  "gs://$BUCKET_EU/air-quality/data" \
  --max-ee-requests=$MAX_EE_REQUESTS \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$REGION_EU" \
  --temp_location="gs://$BUCKET_EU/temp" \
  --service_account_email="112365280327-compute@developer.gserviceaccount.com"
```

## Copenhagen

```sh
python create-dataset.py \
  "copenhagen" \
  "gs://$BUCKET_EU/air-quality/data" \
  --max-ee-requests=$MAX_EE_REQUESTS \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$REGION_EU" \
  --temp_location="gs://$BUCKET_EU/temp" \
  --service_account_email="112365280327-compute@developer.gserviceaccount.com"
```

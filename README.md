# California

```sh
DATA_BUCKET="my-output-data-bucket"
US_BUCKET="my-us-gcs-bucket"
LOCATION="us-central1"

python create-dataset.py \
  "california" \
  "gs://$DATA_BUCKET/air-quality/data" \
  --max-ee-requests=5 \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$LOCATION" \
  --temp_location="gs://$US_BUCKET/temp"
```

# Houston

```sh
DATA_BUCKET="my-output-data-bucket"
US_BUCKET="my-us-gcs-bucket"
LOCATION="us-central1"

python create-dataset.py \
  "houston" \
  "gs://$DATA_BUCKET/air-quality/data" \
  --max-ee-requests=5 \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$LOCATION" \
  --temp_location="gs://$US_BUCKET/temp"
```

# Amsterdam

```sh
DATA_BUCKET="my-output-data-bucket"
EU_BUCKET="my-eu-gcs-bucket"
LOCATION="us-central1"

python create-dataset.py \
  "amsterdam" \
  "gs://$DATA_BUCKET/air-quality/data" \
  --max-ee-requests=5 \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$LOCATION" \
  --temp_location="gs://$EU_BUCKET/temp"
```

# Copenhagen

```sh
DATA_BUCKET="my-output-data-bucket"
EU_BUCKET="my-eu-gcs-bucket"
LOCATION="us-central1"

python create-dataset.py \
  "copenhagen" \
  "gs://$DATA_BUCKET/air-quality/data" \
  --max-ee-requests=5 \
  --max-rows=1000 \
  --runner="DataflowRunner" \
  --project="$PROJECT" \
  --region="$LOCATION" \
  --temp_location="gs://$EU_BUCKET/temp"
```

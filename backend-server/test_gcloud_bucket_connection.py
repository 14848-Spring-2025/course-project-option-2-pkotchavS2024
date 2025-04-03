from google.cloud import storage

# Set the path to your credentials file
credentials_path = "./key.json"

# Set environment variable for authentication
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize storage client
client = storage.Client()

# List buckets
buckets = list(client.list_buckets())
print(f"Buckets: {[bucket.name for bucket in buckets]}")

# Check specific bucket
bucket_name = "emerald-skill-436000-t2_cloudbuild"
bucket = client.bucket(bucket_name)
if bucket.exists():
    print(f"Bucket {bucket_name} exists.")
else:
    print(f"Bucket {bucket_name} does not exist.")
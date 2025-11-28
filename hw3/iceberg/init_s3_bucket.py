from botocore.client import Config
import boto3

BUCKET_NAME = 'bucket'
FILE_NAME = 'forbes_2000_companies_2025.csv'
LOCAL_PATH = f'/iceberg/data/{FILE_NAME}'

print("\nStarting S3 bucket initialization script\n")

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1',
    config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4'),
    use_ssl=False
)

try:
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' created.")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"Bucket '{BUCKET_NAME}' already exists.")

# Upload CSV
s3.upload_file(LOCAL_PATH, BUCKET_NAME, FILE_NAME)
print(f"Uploaded '{FILE_NAME}' to bucket '{BUCKET_NAME}'.")
print("\nS3 bucket initialization completed successfully\n")
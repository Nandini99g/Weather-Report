import os
import json
import boto3
import requests
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
TEAM = os.getenv("TEAM", "teamx")
USERNAME = os.getenv("USERNAME", "userx")
CITY = os.getenv("CITY", "Bengaluru,IN")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")  # Optional (dynamic if not provided)

# AWS client
s3_client = boto3.client("s3", region_name=AWS_REGION)


def get_or_create_bucket(bucket_name: str):
    """Check if bucket exists, else create it with proper settings."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket {bucket_name} already exists, reusing.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ["404", "NoSuchBucket"]:
            print(f"Bucket {bucket_name} not found, creating...")
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )

            # Block public access
            s3_client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )

            # Enable encryption
            s3_client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration={
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                            }
                        }
                    ]
                },
            )
            print(f"✅ Bucket {bucket_name} created with encryption + block public access.")
        else:
            raise


def fetch_weather(city: str, api_key: str, retries=3):
    """Fetch current weather from OpenWeatherMap with retries."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            print(f"✅ Weather data fetched for {city}")
            return resp.json()
        except requests.RequestException as e:
            print(f"⚠️ API request failed (attempt {attempt+1}/{retries}): {e}")
            if attempt == retries - 1:
                raise


def save_and_upload(data: dict, bucket_name: str, city: str):
    """Save weather data to local file and upload to S3 with timestamped key."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    city_clean = city.replace(",", "_").replace(" ", "_").lower()

    # S3 key format
    key = (
        f"raw/{datetime.now().year}/{datetime.now().month:02d}/{datetime.now().day:02d}/"
        f"{city_clean}/{city_clean}_{timestamp}.json"
    )

    # Save locally
    local_path = Path(f"./{city_clean}_{timestamp}.json")
    with open(local_path, "w") as f:
        json.dump(data, f, indent=2)

    # Upload
    try:
        s3_client.upload_file(str(local_path), bucket_name, key)
        print(f"✅ Uploaded {local_path} → s3://{bucket_name}/{key}")
    except ClientError as e:
        print(f"❌ Upload failed: {e}")
    finally:
        if local_path.exists():
            local_path.unlink()


def main():
    if not OPENWEATHER_API_KEY:
        raise ValueError("❌ OPENWEATHER_API_KEY not set in environment!")

    # Decide bucket name
    if BUCKET_NAME:
        bucket_name = BUCKET_NAME.lower()
    else:
        bucket_name = f"weather-data-{TEAM}-{USERNAME}".lower()

    # Create or reuse bucket
    get_or_create_bucket(bucket_name)

    # Fetch weather data
    weather_json = fetch_weather(CITY, OPENWEATHER_API_KEY)

    # Save + upload
    save_and_upload(weather_json, bucket_name, CITY)


if __name__ == "__main__":
    main()

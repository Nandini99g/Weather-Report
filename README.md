# Weather-Report

#  S3 Ingestion Script

This project fetches **current weather data** from the [OpenWeatherMap API](https://openweathermap.org/current) and stores it in an **Amazon S3 bucket**.  
The script is **idempotent**:
- If the S3 bucket already exists → reuse it.
- If not → create it automatically with encryption and blocked public access.
- Each run uploads a new **JSON file** with a timestamped filename.

## ⚙️ Prerequisites

- Python 3.8+
- AWS Account + IAM user with S3 permissions
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) configured:
- 
 aws configure


Permissions required:

s3:CreateBucket

s3:GetBucketLocation

s3:ListAllMyBuckets

s3:ListBucket

s3:PutObject


Cloning the repo:
git clone https://github.com/Nandini99g/<your-repo>.git
cd <your-repo>

Creating a virtual environment:

python3 -m venv venv
source venv/bin/activate

Installinf dependencies:
boto3

Environment Variables
.env and fill values:

AWS_REGION=ap-south-1
TEAM=teamb
USERNAME=nandini
CITY=Bengaluru,IN
OPENWEATHER_API_KEY=your_openweather_api_key
BUCKET_NAME=weather-data-teamb-nandini-20250902

Running the Script
python weather_to_s3.py

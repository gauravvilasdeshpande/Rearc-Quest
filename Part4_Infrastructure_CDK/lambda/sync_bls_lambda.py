import boto3
import requests
from bs4 import BeautifulSoup
import os

s3 = boto3.client('s3')


BUCKET_NAME = os.environ.get('BUCKET_NAME', 'your-s3-bucket-name')
BLS_URL = 'https://download.bls.gov/pub/time.series/pr/'
HEADERS = {
    'User-Agent': 'your_email@example.com - rearc challenge lambda'
}
FOLDER = 'bls-data/'

def list_s3_files():
    paginator = s3.get_paginator('list_objects_v2')
    s3_keys = set()
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=FOLDER):
        for obj in page.get('Contents', []):
            key = obj['Key']
            s3_keys.add(key.replace(FOLDER, ''))
    return s3_keys

def handler(event, context):
    print("Starting BLS data sync...")

    # Get the current files in S3
    existing_files = list_s3_files()
    print(f"Found {len(existing_files)} files in S3.")

    # Scrape the BLS directory for available files
    try:
        response = requests.get(BLS_URL, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    files_to_download = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.Current')]

    for filename in files_to_download:
        if filename not in existing_files:
            print(f"Downloading {filename}")
            file_url = BLS_URL + filename
            file_resp = requests.get(file_url, headers=HEADERS)
            if file_resp.status_code == 200:
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=FOLDER + filename,
                    Body=file_resp.content
                )
                print(f"Uploaded {filename} to S3.")
            else:
                print(f"Failed to download {filename} - status code {file_resp.status_code}")
        else:
            print(f"{filename} already exists in S3. Skipping.")

    print("Sync complete.")

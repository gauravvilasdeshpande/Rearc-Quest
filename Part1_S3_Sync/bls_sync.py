import requests
import boto3
from bs4 import BeautifulSoup

BUCKET_NAME = 'your-s3-bucket-name'
SOURCE_URL = 'https://download.bls.gov/pub/time.series/pr/'
FOLDER = 'bls-data/'
HEADERS = {'User-Agent': 'your_email@example.com - data ingestion'}

s3 = boto3.client('s3')

def list_s3_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER)
    return {obj['Key'].replace(FOLDER, '') for obj in response.get('Contents', [])}

def sync_bls():
    r = requests.get(SOURCE_URL, headers=HEADERS)
    soup = BeautifulSoup(r.text, 'html.parser')
    file_links = [a['href'] for a in soup.find_all('a') if a['href'].endswith('.Current')]

    s3_files = list_s3_files()
    for file in file_links:
        if file not in s3_files:
            file_url = f"{SOURCE_URL}{file}"
            file_data = requests.get(file_url, headers=HEADERS).content
            s3.put_object(Bucket=BUCKET_NAME, Key=FOLDER + file, Body=file_data)
            print(f"Uploaded: {file}")

if __name__ == "__main__":
    sync_bls()
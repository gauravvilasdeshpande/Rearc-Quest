import requests
import json
import boto3

BUCKET_NAME = 'your-s3-bucket-name'
KEY = 'us-population.json'
API_URL = 'https://api.census.gov/data/timeseries/popest/national/pop'
PARAMS = {'get': 'POP', 'time': 'from+2013+to+2018'}

def fetch_and_upload():
    r = requests.get(API_URL, params=PARAMS)
    data = r.json()

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=KEY,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print("Population data uploaded.")

if __name__ == "__main__":
    fetch_and_upload()

import boto3
import pandas as pd
import json
import os

s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get('BUCKET_NAME', 'your-s3-bucket-name')
BLS_KEY = 'bls-data/pr.data.0.Current'
POP_JSON_KEY = 'us-population.json'

def download_s3_file(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    return response['Body'].read().decode('utf-8')

def parse_population_json(content):
    raw = json.loads(content)
    headers, *rows = raw
    df = pd.DataFrame(rows, columns=headers)
    df['POP'] = df['POP'].astype(int)
    df['time'] = df['time'].astype(int)
    return df

def handler(event, context):
    print("Running analytics lambda...")

    try:
        # Load both files
        bls_data = download_s3_file(BLS_KEY)
        pop_data = download_s3_file(POP_JSON_KEY)

        # Load into DataFrames
        bls_df = pd.read_csv(pd.compat.StringIO(bls_data), delim_whitespace=True)
        bls_df['value'] = pd.to_numeric(bls_df['value'], errors='coerce')
        bls_df = bls_df[bls_df['period'].str.startswith('Q')]

        pop_df = parse_population_json(pop_data)

        # 1. Population stats 2013–2018
        subset = pop_df[pop_df['time'].between(2013, 2018)]
        mean_pop = subset['POP'].mean()
        std_pop = subset['POP'].std()
        print(f"[Population] Mean: {mean_pop}, Std Dev: {std_pop}")

        # 2. Best year per series_id
        grouped = bls_df.groupby(['series_id', 'year'])['value'].sum().reset_index()
        best = grouped.loc[grouped.groupby('series_id')['value'].idxmax()]
        print("[Best Year per series_id]")
        print(best.head())

        # 3. Join for PRS30006032-Q01
        filtered = bls_df[(bls_df['series_id'] == 'PRS30006032') & (bls_df['period'] == 'Q01')]
        filtered['year'] = filtered['year'].astype(int)
        joined = filtered.merge(pop_df, left_on='year', right_on='time', how='left')
        joined = joined[['series_id', 'year', 'period', 'value', 'POP']].rename(columns={'POP': 'Population'})
        print("[Joined population & BLS for PRS30006032 / Q01]")
        print(joined)

    except Exception as e:
        print(f"Error in analytics lambda: {str(e)}")

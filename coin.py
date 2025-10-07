#coin.py
import pandas as pd
import boto3
import s3fs
from requests import Session
from datetime import datetime
from airflow.models import Variable

URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
S3_BUCKET = Variable.get("S3_BUCKET")
API_KEY = Variable.get("API_KEY")


def coin_handler(**context):

    parameters = {'start': '1', 'limit': '100', 'convert': 'USD'}
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY
    }

    session = Session()
    session.headers.update(headers)

    response = session.get(URL, params=parameters, timeout=20)
    response.raise_for_status()

    data = response.json().get("data", [])

    df = pd.json_normalize(data, sep='_')
    df = df[['id','name','symbol','cmc_rank',
             'quote_USD_price','quote_USD_market_cap',
             'quote_USD_volume_24h','quote_USD_percent_change_24h']]
    

    # Generate a unique file name using current date and time
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"coindata/coinmarketcap_data_{timestamp}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=file_name,
        Body=csv_bytes,
        ContentType="text/csv"
    )

    print(f"Upload successful: s3://{S3_BUCKET}/{file_name}")
    return {"statusCode": 200, "body": "Upload successful"}

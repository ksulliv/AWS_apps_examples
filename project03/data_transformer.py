import json
import boto3
import random
import datetime
import yfinance as yf
from time import sleep

kinesis = boto3.client('kinesis', "us-east-2")

tickers=['AMZN', 'BABA', 'WMT', 'EBAY', 'SHOP','TGT','BBY','HD','COST','KR']
start = "2022-10-22"
end = "2022-11-05"
interval = "5m"

def lambda_handler(event, context):
    for ticker in tickers:
        data = yf.Ticker(ticker).history(start=start, end=end, interval=interval)
        for datetime, rows in data.iterrows():
            output = {}
            output['high'] = round(rows.High,2)
            output['low'] = round(rows.Low,2)
            output['volatility'] = round(rows.High-rows.Low,2)
            output["ts"] = str(datetime)
            output['name'] = ticker
            data = json.dumps(output)+"\n"
            print(data)
            kinesis.put_record(
                    StreamName="project3-stream",
                    Data=data,
                    PartitionKey="partitionkey")
            sleep(0.05)
            
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }

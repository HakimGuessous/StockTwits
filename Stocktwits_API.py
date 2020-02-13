import json
import requests
import pandas as pd
import numpy as np
import time
import datetime
import threading
import os
from gcloud import storage

#Function to pull tweets from the StockTwits API
def get_twits(Ticker):
    response = requests.get(f'https://api.stocktwits.com/api/2/streams/symbol/{Ticker}.json')
    if response.status_code == 200:
        return json.loads(response.content.decode('utf-8'))
    else:
        return None

#Tickers to iterate over
Tickers = ['GE','AMD','F','AAPL','MU','MSFT','T','BA','QCOM','INTC','DIS','TWTR','JPM','FB','MS','ATVI',
          'NVDA','ORCL','JNJ','NFLX','GM','V','EA','AMZN','BTC.X','TSLA','SPY','ETH.X']


id = pd.DataFrame({"Ticker":Tickers,
                   "ID":0})
#Converts JSON structure to csv and will save new tweets in project directory
def get_data(id):
    output = pd.DataFrame()
    for i in range(0,len(Tickers)):
        try:
            twits = get_twits(Tickers[i])
            twits2 = pd.DataFrame(list(twits['messages']))[['id', 'body', 'created_at']]
            twits2['ticker'] = Tickers[i]
            twits2['user'] = ''
            twits2['followers'] = ''
            twits2['likes'] = ''
            twits2['sentiment'] = ''
            for j in range(0,len(twits2)):
                twits2.loc[j,'user'] = twits['messages'][j]['user']['username']
                twits2.loc[j,'followers'] = twits['messages'][j]['user']['followers']
                try:
                    twits2.loc[j, 'likes'] = twits['messages'][j]['likes']['total']
                except:
                    twits2.loc[j, 'likes'] = 0
                try:
                    twits2.loc[j,'sentiment'] = twits['messages'][j]['entities']['sentiment']['basic']
                except:
                    twits2.loc[j,'sentiment'] = 'None'

            twits2 = twits2.loc[twits2['id'] > int(id.loc[id['Ticker'] == Tickers[i],'ID']), :]

            if len(twits2) > 0: id.at[i,'ID'] = twits2['id'].max()

            if len(twits2) > 0: output = output.append(twits2)
        except:
            print('Could not get twits for',Tickers[i])
    if len(output) > 0:
        output = output.reset_index(drop=True)
        if not os.path.exists(os.path.join(os.getcwd(), 'twits/output.csv')):
            output.to_csv(os.path.join(os.getcwd(), 'twits/output.csv'), index=False)
        else:
            with open(os.path.join(os.getcwd(), 'twits/output.csv'), 'a', newline='', encoding="utf-8") as f:
                output.to_csv(f, header=False, index=False)

    return id

#Upload csv into Google Cloud Storage
def upload_blob():
  """Uploads a file to the bucket."""
  storage_client = storage.Client.from_service_account_json(os.path.join(os.getcwd(), 'crypto-trading-c8a8078ea295.json'))
  bucket = storage_client.get_bucket('stock-twits')
  blob_name = str(datetime.datetime.now().year)+str(datetime.datetime.now().month)+str(datetime.datetime.now().day)+'_twits.csv'
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(os.path.join(os.getcwd(), 'twits/output.csv'))
  if storage.Blob(bucket=bucket, name=blob_name).exists(storage_client):
      os.remove(os.path.join(os.getcwd(), 'twits/output.csv'))

      print('File {} uploaded to {}.'.format(
          blob_name,
          'stock-twits bucket'))


if not os.path.exists(os.path.join(os.getcwd(), 'twits')):
    os.mkdir(os.path.join(os.getcwd(), 'twits'))


#Timer set to get new tweets every 10 minutes
t = None
def startTwits(id):
    global t
    if datetime.datetime.now().minute in [0, 1]:
        print(datetime.datetime.now(), "- Stock twits still active")

    if datetime.datetime.now().hour == 1 and datetime.datetime.now().minute in [0, 1]:
        upload_blob()

    id = get_data(id)

    time.sleep((10 - datetime.datetime.now().minute % 10)*60)
    t = threading.Timer(5, startTwits(id))
    t.start()
    return id

id = startTwits(id)























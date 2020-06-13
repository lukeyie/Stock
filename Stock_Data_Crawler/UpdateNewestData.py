import time
import requests
from io import StringIO
import pandas as pd
import numpy as np 
from pymongo import MongoClient
from pymongo import cursor
import json

def CreateLatestTimeStamp(stockData) :
    latestDay = stockData['DayInfo'][-1]['Date']
    latestDay = int(time.mktime(time.strptime(latestDay, "%Y-%m-%d")))
    return latestDay + 24*3600

def CreateTomorrowTimeStamp() :
    today = time.strftime("%Y-%m-%d",time.gmtime())
    today = int(time.mktime(time.strptime(today, "%Y-%m-%d")))
    return today + 24*3600

def GetStocksInfo(stockCurser, startTime, endTime) :
    stocksDF = pd.DataFrame()
    ticker = stockCurser['Ticker']
    stockName = stockCurser['StockName']
    print('## Info: Download Ticker '+ticker+'!')
    site = "https://query1.finance.yahoo.com/v7/finance/download/" + ticker + ".TW?period1=" + str(startTime) + "&period2=" + str(endTime) + "&interval=1d&events=history&crumb=hP2rOschxO0"
    try:
        response = requests.get(site)
        tmp_df = pd.read_csv(StringIO(response.text))
        tmp_df['Ticker'] = ticker
        tmp_df['StockName'] = stockName
        del tmp_df['Adj Close']
        stocksDF = pd.concat([stocksDF,tmp_df],axis=0)
    except:
        print('## Warning: Ticker '+ticker+' is failed!')
    
    return stocksDF

# connect db
userId = "lukechen"
password = "Glitch2582"
clusterName = "lukefreecluster"
dbName = "TWStocksDB"
collectionName = "StocksInfo"

conn = MongoClient("mongodb+srv://"+userId+":"+password+"@"+clusterName+"-1kypv.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = conn[dbName]
collection = db[collectionName]

for stockData in collection.find():
    startTime = CreateLatestTimeStamp(stockData)
    endTime= CreateTomorrowTimeStamp()
    stockDF = GetStocksInfo(stockData, startTime, endTime)

    for i in range(stockDF.shape[0]) :
        condition = ({
            "Ticker": stockDF.iloc[i]["Ticker"],
            "StockName": stockDF.iloc[i]["StockName"],
        })

        data = ({
            "DayInfo":
                {
                    "Date": stockDF.iloc[i]["Date"],
                    "Open": stockDF.iloc[i]["Open"],
                    "Close": stockDF.iloc[i]["Close"],
                    "High": stockDF.iloc[i]["High"],
                    "Low": stockDF.iloc[i]["Low"]
                }
        })
        collection.update(condition, {'$addToSet': data}, upsert=True)
    
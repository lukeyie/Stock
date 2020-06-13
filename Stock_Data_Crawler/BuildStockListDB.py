import time
import requests
from io import StringIO
import pandas as pd
import numpy as np 
from pymongo import MongoClient
import json

def CreateStartDayTimeStamp(startDay) :
    startDay = startDay
    startDay = int(time.mktime(time.strptime(startDay, "%Y-%m-%d")))
    return startDay

def CreateTomorrowTimeStamp() :
    today = time.strftime("%Y-%m-%d",time.gmtime())
    today = int(time.mktime(time.strptime(today, "%Y-%m-%d")))
    return today + 24*3600

def GetTWStockList() :
    res = requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=2")
    resDF = pd.read_html(res.text)[0]

    notStockStartIndex = 0
    for i in range(resDF.shape[0]) :
        if resDF.iloc[i][0] == '上市認購(售)權證' :
            notStockStartIndex = i
            break
    stockListDF = resDF.iloc[2:notStockStartIndex]
    stockListDF.columns = resDF.iloc[0]
    stockListDF = stockListDF.reset_index(drop=True)
    
    newStockListDF = stockListDF['有價證券代號及名稱'].str.replace(u'\u3000',' ').str.split(u' ',expand=True)
    newStockListDF.columns = ['Ticker', 'StockName']
    newStockListDF['Sector'] = stockListDF['產業別']

    return newStockListDF

def GetStocksInfo(stockList, startTime, endTime) :
    stocksDF = pd.DataFrame()
    for i in range(stockList.shape[0]) :
        ticker = stockList.iloc[i]['Ticker']
        stockName = stockList.iloc[i]['StockName']
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

# create timestamp
startDayTimestamp = CreateStartDayTimeStamp("2019-01-01")
tomorrowTimeStamp = CreateTomorrowTimeStamp()

# get Taiwan stock list
stockList = GetTWStockList()
allStocksInfo = GetStocksInfo(stockList, startDayTimestamp, tomorrowTimeStamp)

# connect db
userId = "lukechen"
password = "Glitch2582"
clusterName = "lukefreecluster"
dbName = "TWStocksDB"
collectionName = "StocksInfo"

conn = MongoClient("mongodb+srv://"+userId+":"+password+"@"+clusterName+"-1kypv.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = conn[dbName]
collection = db[collectionName]

# insert data to db
for i in range(allStocksInfo.shape[0]) :
    condition = ({
        "Ticker": allStocksInfo.iloc[i]["Ticker"],
        "StockName": allStocksInfo.iloc[i]["StockName"],
    })

    data = ({
        "DayInfo":
            {
                "Date": allStocksInfo.iloc[i]["Date"],
                "Open": allStocksInfo.iloc[i]["Open"],
                "Close": allStocksInfo.iloc[i]["Close"],
                "High": allStocksInfo.iloc[i]["High"],
                "Low": allStocksInfo.iloc[i]["Low"]
            }
    })

    collection.update(condition, {'$addToSet': data}, upsert=True)

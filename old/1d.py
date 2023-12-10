from ib_insync import *
import json
import redis
import _thread
import time
from datetime import datetime
import sys
import os
from dateutil.relativedelta import relativedelta
import signal
import requests
# util.startLoop()  # uncomment this line when in a notebook

import pytz

tz_NY = pytz.timezone('America/New_York') 

datetime_NY = datetime.now(tz_NY)

print("NY time:", datetime_NY.strftime("%H:%M:%S"))
forex_symbol=["USDSEK","AUDCHF","GBPJPY","ZARJPY","AUDCNH","EURRUB","GBPNZD","CHFNOK","EURPLN","CADPLN","EURCHF","EURHUF","GBPUSD","USDNOK","AUDCAD","USDPLN","GBPMXN","EURDKK","AUDJPY","USDILS","EURNZD","AUDSGD","EURUSD","EURSGD","CADCHF","USDMXN","GBPPLN","USDCHF","EURNOK","CADNOK","EURSEK","EURAUD","GBPZAR","USDHKD","CNHJPY","USDZAR","NOKDKK","CHFPLN","NZDCAD","AUDUSD","GBPCAD","EURTRY","EURCNH","EURHKD","EURJPY","HKDJPY","USDCNH","CADJPY","NZDJPY","USDCAD","TRYJPY","USDSGD","GBPNOK","AUDPLN","USDHUF","SGDHKD","USDTRY","EURMXN","XAGUSD","CHFHUF","USDTHB","EURGBP","GBPDKK","EURCZK","NOKJPY","USDRUB","NZDCHF","AUDNOK","EURCAD","USDCZK","EURZAR","SGDJPY","USDJPY","GBPSEK","XAUUSD","GBPCHF","USDDKK","GBPHKD","NOKSEK","NZDUSD","GBPAUD","AUDNZD","CHFJPY","GBPSGD"]

def stockprice(bars,symbol):

    r = redis.Redis(host='149.28.120.38', port=6379,password='6$gtA453', db=10)
    print('thread symbol %s is start' %symbol)
    x=[]
    for bar in bars:
    
        x.insert(0,{
        "date": bar.date.strftime("%Y-%m-%d %H:%M:%S"),
        "open": bar.open,
        "high": bar.high,
        "low" : bar.low,
        "close" : bar.close,
        "volume" : int(bar.volume)*100, 
        })

    y = json.dumps(x)
    r.set(symbol, y )
    r.connection_pool.disconnect()
    print('thread symbol %s is finish' %symbol)
    #print(y)
    
def exit():
    os.kill(os.getpid(), signal.SIGTERM)
    print('{} Now the system will to sleep for Next Day '.datetime.now(tz_NY).strftime("%H:%M:%S")) #this works ok
  
    a = datetime.now(tz_NY)
    b = datetime.now(tz_NY)+ relativedelta(days=1)
    b = b.strftime('%Y-%m-%d 09:30:00')
    b = datetime.strptime(b, "%Y-%m-%d %H:%M:%S")
    #print(type(b))
    c=(b-a).total_seconds()
    time.sleep(c)

def checkTimeSync():
    #return False
    minutes=int(datetime.now(tz_NY).strftime("%M"))
    sec=int( datetime.now().strftime("%S"))
    if minutes%1440!=0 or sec!=1:
        return True
    else:
        return False


def getSymbols(interval):
    url = 'https://panel.scanical.com/api/get_symbols/'+interval
    x = requests.get(url)
    return x.json()

def getContract(symbol):
    primary_nasdaq=['CSCO']
    
    if symbol in primary_nasdaq:
        contract = Stock(symbol.lower(), 'SMART', 'USD', primaryExchange='NASDAQ')
    elif symbol in forex_symbol:
        contract = Forex(symbol.lower())
    else:
        contract = Stock(symbol.lower(), 'SMART', 'USD')

    return contract






ib = IB()
client=60
ib.connect('127.0.0.1', 7497, clientId=client,timeout=10)
claster=getSymbols('1d')
for symbol in claster:
    contract=getContract(symbol)

    if symbol in forex_symbol:
        candelShow='MIDPOINT'
    else:
        candelShow='TRADES'

    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='200 D',
        barSizeSetting='1 day', whatToShow=candelShow, useRTH=True)
    _thread.start_new_thread(stockprice,(bars,symbol,))

    
while checkTimeSync():
    time.sleep(1)
    print("Timing synchronization at "+datetime_NY.strftime("%H:%M")+":"+datetime.now().strftime("%S"))

print("End of synchronization")


while True:
    start_time = time.time()
    try:
        claster=getSymbols('1d')
        
        for symbol in claster:
            contract=getContract(symbol)
            if symbol in forex_symbol:
                candelShow='MIDPOINT'
            else:
                candelShow='TRADES'

            bars = ib.reqHistoricalData(
                contract, endDateTime='', durationStr='200 D',
                barSizeSetting='1 day', whatToShow=candelShow, useRTH=True)

            _thread.start_new_thread(stockprice,(bars,symbol,))
    except:
        try:
            print("try to connect again")
            ib.connect('127.0.0.1', 7497, clientId=client,timeout=10)
        except:
            print("ib have problem with connection")
    print("--- %s seconds ---" % (time.time() - start_time))
    
    if 3600-int(time.time() - start_time)>0:
        time.sleep(3600-int(time.time() - start_time))
ib.disconnect()

from ib_insync import *
import json
import redis
import _thread
import time
import asyncio
import schedule
from datetime import datetime
import sys
import os
from dateutil.relativedelta import relativedelta
import signal

# util.startLoop()  # uncomment this line when in a notebook

import pytz


tz_NY = pytz.timezone('America/New_York') 

datetime_NY = datetime.now(tz_NY)

print("NY time:", datetime_NY.strftime("%H:%M:%S"))


def stockprice(bars,symbol):

    # db 1 is daily 
    
    r = redis.Redis(host='149.28.120.38', port=6379,password='6$gtA453', db=11)
    print('thread symbol %s is start' %symbol)
    x=[]
    for bar in bars:
    
        x.insert(0,{
        "date": bar.date.strftime("%Y-%m-%d"),
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

    #print(datetime.now().strftime("%S"))
    minutes=int(datetime.now(tz_NY).strftime("%M"))
    sec=int( datetime.now().strftime("%S"))
    if sec!=1:
        return True
    else:
        return False






while checkTimeSync():
    time.sleep(1)
    print("Timing synchronization at "+datetime_NY.strftime("%H:%M")+":"+datetime.now().strftime("%S"))


print("End of synchronization")
schedule.every().day.at('16:01').do(exit)


ib = IB()

# daily client number 1
client=61

ib.connect('127.0.0.1', 7497, clientId=client,timeout=10)
claster=['SPY','ECL','LYB','NEM','DIS','EA','FB','GOOG','MTCH','NFLX','PINS','ROKU','TWLO','Z','QCOM','SNOW','SQ','TTD','VMW','WDAY','ZS','ZEN','SPOT','VEEV','BBY','CHWY','CVNA','DRI','ETSY','HD','LEN','LOW','MAR','MCD','NKE','PTON','RCL','AEP','D','DUK','ED','ES','LNT','CMS','BABA','NOW','CL','COST','CLX','DG','DLTR','PG','STZ','TSN','WMT','TSLA','CVX','COF','GS','ICE','MMC','PYPL','STT','V','ABBV','ANTM','BIIB','EW','BA','CAT','CPRT','FDX','GPN','LHX','LMT','UNP','WM','DLR','PLD','SPG','AAPL','ADBE','ADI','AMD','AMAT','AVGO','COUP','CRM','CRWD','CTSH','CTXS','DOCU','ENPH','FIS','KLAC','LRCX','MCHP','MSFT','MU','NOW','NVDA','OKTA']
while True:
    start_time = time.time()
    try:

        for symbol in claster:
            contract = Stock(symbol.lower(), 'SMART', 'USD')
            bars = ib.reqHistoricalData(
                contract, endDateTime='', durationStr='20 D',
                barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
        


            _thread.start_new_thread(stockprice,(bars,symbol,))

    except:
        try:
            print("try to connect again")
            ib.connect('127.0.0.1', 7497, clientId=client,timeout=10)
        except:
            print("ib have problem with connection")

            
    print("--- %s seconds ---" % (time.time() - start_time))
    schedule.run_pending()
    if 86400-int(time.time() - start_time)>0:
        time.sleep(86400-int(time.time() - start_time))
ib.disconnect()

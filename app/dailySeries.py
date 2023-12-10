from ib_insync import *
import json
import redis
import _thread
from random import randint
from time import sleep,time
from datetime import datetime
import sys
import os
from dateutil.relativedelta import relativedelta
import pytz
import requests
from binance.client import Client
import asyncio
import pika
import threading

class PriceSeries:
    def __init__(self, client, interval):
            self.tz_NY = pytz.timezone('America/New_York') 
            self.client = client
            self.interval = interval
            self.jobs =[]
            self.api_key="Hnt7HPS4RjJehhPRr6VqUTzOr5hNsCBOfBpCQs5LlXueAE7wzYce0QUNPAHEgoo0"
            self.api_secret="SKeHvrGe3Sbok35e4G4U9PEK5cIo1raKXmIJxRCZTOGatmuy6m84aRWWBrhrzEU2"
            self.forex_symbol=self.getExchangeSymbols('IB')
            self.crypto_symbol=self.getExchangeSymbols('Binance')
            self.primary_nasdaq=self.getExchangeSymbols('Nasdaq')
            self.primary_nyse=self.getExchangeSymbols('Nyse')
            self.rabbbitmq_user="scanical_rabbitmql"
            self.rabbbitmq_password="ZZz&eJ2=RG"
            self.rabbbitmq_host='155.138.135.110'
            self.ib = IB()
            self.connect()
            self.connect_binance()
            self.threads=[]
    def connect(self):
        while True:
            try:
                self.ib.connect('127.0.0.1', 7497, clientId=self.client,timeout=10)
                break
            except:
                print('ib connection error')
                sleep(35)
                continue

    def connect_binance(self):
        try:
            self.crypto_client = Client(self.api_key, self.api_secret)
        except:
            self.crypto_client = Client(self.api_key, self.api_secret)

    def database_number(self):
        return 10

    def crypto_price(self,symbol, kline_interval,contract):
        try:
            bars =contract.get_klines(symbol=symbol, interval=kline_interval)
        except:
            sleep(randint(1,10))    
            bars =contract.get_klines(symbol=symbol, interval=kline_interval)
        r = redis.Redis(host='155.138.135.110', port=6379,password='6$gtA453', db=self.database_number())
        print('thread symbol %s is start' %symbol)
        x=[]
        for bar in bars:
            
            timestamp =datetime.fromtimestamp(bar[0]/1000)
            x.insert(0,{
            "date": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(bar[1]),
            "high": float(bar[2]),
            "low" : float(bar[3]),
            "close" : float(bar[4]),
            "volume" : float(bar[5]), 
            })

        y = json.dumps(x)
        r.set(symbol, y )
        r.connection_pool.disconnect()
        print('thread symbol %s is finish' %symbol)
        #print(y)

    def stockprice(self,bars,symbol):

        print('thread symbol %s is start' %symbol)
        x=[]

        if len(bars)==0:
            print('symbol %s is empty' %symbol)
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
        r = redis.Redis(host='155.138.135.110', port=6379,password='6$gtA453', db=self.database_number())

        r.set(symbol, y )
        r.connection_pool.disconnect()
        print('thread symbol %s is finish' %symbol)
        #print(y)
                
    def getSymbols(self):
        url = 'https://panel.scanical.com/api/symbols/snapshot'
        try:
            x = requests.get(url)
            return x.json()
        except:
            return []
    def getContractForex(self,symbols):
        try:
            contracts = [Forex(pair) for pair in symbols]
            self.ib.qualifyContracts(*contracts)        
        except:
            self.connect()
            contracts = [Forex(pair) for pair in symbols]
            self.ib.qualifyContracts(*contracts)    
            pass
        
        return contracts
    def getContractNasdaq(self,symbols):
        contracts = [Stock(pair, 'SMART', 'USD', primaryExchange='NASDAQ') for pair in symbols]
        self.ib.qualifyContracts(*contracts)        
   
        return contracts
    def getContractNyse(self,symbols):
        contracts = [Stock(pair, 'SMART', 'USD', primaryExchange='NYSE') for pair in symbols]
        self.ib.qualifyContracts(*contracts)        
   
        return contracts
    def getContractCrypto(self):
        contract = self.crypto_client        
   
        return contract       
    def get_kline_interval(self,interval):

        if interval=='1min':
            return Client.KLINE_INTERVAL_1MINUTE
        elif interval=='5min':
            return Client.KLINE_INTERVAL_5MINUTE
        elif interval=='15min':
            return Client.KLINE_INTERVAL_15MINUTE
        elif interval=='30min':
            return Client.KLINE_INTERVAL_30MINUTE
        elif interval=='1h':
            return Client.KLINE_INTERVAL_1HOUR
        elif interval=='2h':
            return Client.KLINE_INTERVAL_1HOUR
        elif interval=='4h':
            return Client.KLINE_INTERVAL_1HOUR
        elif interval=='6h':
            return Client.KLINE_INTERVAL_1HOUR
        elif interval=='8h':
            return Client.KLINE_INTERVAL_1HOUR
        elif interval=='12h':
            return Client.KLINE_INTERVAL_1HOUR
        elif interval=='1d':
            return Client.KLINE_INTERVAL_1DAY
        elif interval=='1w':
            return Client.KLINE_INTERVAL_1WEEK
        elif interval=='1m':
            return Client.KLINE_INTERVAL_1MONTH

    def get_bar_size(self,interval):

        if interval=='1min':
            return "1 min"
        elif interval=='5min':
            return "5 mins"
        elif interval=='15min':
            return "15 mins"
        elif interval=='30min':
            return "30 mins"
        elif interval=='1h':
            return "1 hour"
        elif interval=='2h':
            return "2 hours"
        elif interval=='4h':
            return "4 hours"
        elif interval=='6h':
            return "6 hours"
        elif interval=='8h':
            return "8 hours"
        elif interval=='12h':
            return "12 hours"
        elif interval=='1d':
            return '1 day'
        elif interval=='1w':
            return '1 week'
        elif interval=='1m':
            return '1 month'

    def get_kline_duration(self,interval):
        
        if interval=='1min':
            return "1 day ago UTC"
        elif interval=='5min':
            return "1 day ago UTC"
        elif interval=='15min':
            return "1 day ago UTC"
        elif interval=='30min':
            return "2 day ago UTC"
        elif interval=='1h':
            return "4 day ago UTC"
        elif interval=='2h':
            return "8 day ago UTC"
        elif interval=='4h':
            return "16 day ago UTC"
        elif interval=='6h':
            return "50 day ago UTC"
        elif interval=='8h':
            return "70 day ago UTC"
        elif interval=='12h':
            return "110 day ago UTC"
        elif interval=='1d':
            return "220 day ago UTC"
        elif interval=='1w':
            return "4 year ago UTC"
        elif interval=='1m':
            return "16 year ago UTC"
    def get_ib_duration(self,interval):
        if interval=='1min':
            return "2 D"
        elif interval=='5min':
            return "1 D"
        elif interval=='10min':
            return "3 D"
        elif interval=='15min':
            return "3 D"
        elif interval=='30min':
            return "5 D"
        elif interval=='1h':
            return "9 D"
        elif interval=='2h':
            return "17 D"
        elif interval=='4h':
            return "1 M"
        elif interval=='6h':
            return "3 M"
        elif interval=='8h':
            return "3 M"
        elif interval=='12h':
            return "200 D"
        elif interval=='1d':
            return "200 D"
        elif interval=='1w':
            return "4 Y"
        elif interval=='1m':
            return "17 Y"
    def checkTimeSync(self,interval):
        #return False
        minutes=int(datetime.now(self.tz_NY).strftime("%M"))
        sec=int( datetime.now().strftime("%S"))
 
        if minutes%1!=0 or sec!=1:
            return True
        else:
            return False


    def getExchangeSymbols(self,exchnage):
        url = 'https://panel.scanical.com/api/exchangesymbols/'+exchnage
        try:
            x = requests.get(url)
            return x.json()
        except:
            return []
       
    def update_bar(self):
        clasters=self.getSymbols()
        for i in range(0, len(clasters), 500):
            claster = clasters[i:i + 500]
            exist_forex_symbol= list(set(claster).intersection(set(self.forex_symbol)) )
            exist_nasdaq_symbol= list(set(claster).intersection(set(self.primary_nasdaq)) )
            exist_nyse_symbol= list(set(claster).intersection(set(self.primary_nyse)) )
            exist_crypto_symbol= list(set(claster).intersection(set(self.crypto_symbol)) )
            # for test
            #exist_forex_symbol=["EURUSD","USDJPY"]
            #exist_nyse_symbol=["A","PG"]
            #exist_crypto_symbol=["BTCUSDT"]
            

            if exist_forex_symbol:
                contracts=self.getContractForex(exist_forex_symbol)
                self.threads=[]

                candelShow='MIDPOINT'
                for contract in contracts:
                    try:
                        bars =self.ib.reqHistoricalData(
                            contract, endDateTime='', durationStr="201 D",
                            barSizeSetting='1 day', whatToShow=candelShow, useRTH=True)
                        #self.ib.sleep(3)
                        #print(contract.symbol+contract.currency)
                        t= threading.Thread(target=self.stockprice,args=(bars,contract.symbol+contract.currency,))
                        self.threads.append(t)
                        t.start()
                        #_thread.start_new_thread(self.stockprice,(bars,symbol,))
                    except:
                        print("try to connect again")
                        self.connect()

            if exist_nasdaq_symbol:
                contracts=self.getContractNasdaq(exist_nasdaq_symbol)
                self.threads=[]

            
                for contract in contracts:
                    candelShow='TRADES'
                    try:
                        bars =self.ib.reqHistoricalData(
                            contract, endDateTime='', durationStr="201 D",
                            barSizeSetting='1 day', whatToShow=candelShow, useRTH=True)
                        #self.ib.sleep(3)
                        t= threading.Thread(target=self.stockprice,args=(bars,contract.symbol,))
                        self.threads.append(t)
                        t.start()
                    except:
                        try:
                            print("try to connect again")
                            self.connect()   
                        except Exception as e:
                            print("ib have problem with connection")
                            print(e)  
            if exist_nyse_symbol:
                contracts=self.getContractNyse(exist_nyse_symbol)
                self.threads=[]

            
                for contract in contracts:
                    candelShow='TRADES'
                    try:
                        bars =self.ib.reqHistoricalData(
                            contract, endDateTime='', durationStr="201 D",
                            barSizeSetting='1 day', whatToShow=candelShow, useRTH=True)
                        #self.ib.sleep(3)
                        t= threading.Thread(target=self.stockprice,args=(bars,contract.symbol,))
                        self.threads.append(t)
                        t.start()
                    except:
                        try:
                            print("try to connect again")
                            self.connect()   
                        except Exception as e:
                            print("ib have problem with connection")
                            print(e)  
            if exist_crypto_symbol:
                self.threads=[]
                contract=self.getContractCrypto()
                for crypto_symbol in exist_crypto_symbol:
                    try:
                        #bars =contract.get_historical_klines(symbol, kline_interval,start_str=duration)
                        #_thread.start_new_thread(self.crypto_price,(bars,symbol,))
                        #_thread.start_new_thread(self.crypto_price,(symbol, kline_interval,duration,contract,))
                        t= threading.Thread(target=self.crypto_price,args=(crypto_symbol, Client.KLINE_INTERVAL_1DAY,contract,))
                        self.threads.append(t)
                        t.start()
                    
                    except Exception as e:
                        print("try to connect binance again:",e)
                        self.connect_binance()
                    
            sleep(60)

tz_NY = pytz.timezone('America/New_York') 
datetime_NY = datetime.now(tz_NY)



client=100
interval="snap"
sleep_loop_time=3*60*60

Engine=PriceSeries(client,interval)
Engine.update_bar()

while Engine.checkTimeSync(interval):
    
    sleep(1)
    print("Timing synchronization at "+datetime.now().strftime("%H:%M:%S"))


print("End of synchronization")




while True:
    
    start_time = time()

    Engine.update_bar()
    

    print("%s --- %s seconds ---" % (datetime.now(),time() - start_time))
    
    if sleep_loop_time-(time() - start_time)>0:
        sleep(sleep_loop_time-(time() - start_time))
ib.disconnect()

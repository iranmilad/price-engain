from ib_insync import *
import json
import redis
import _thread
from random import randint
from time import sleep
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
import MetaTrader5 as mt5

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
            self.commodity_symbol=["XAUUSD"]
            self.rabbbitmq_user="scanical_rabbitmql"
            self.rabbbitmq_password="ZZz&eJ2=RG"
            self.rabbbitmq_host='127.0.0.1'
            self.ib = IB()
            self.connect()
            self.connect_binance()
            self.connect_metatrader()
            self.threads=[]

    def isTreadAlive(self):
        for t in self.threads:
            if t.is_alive():
                return True
        return False
    def connect_rabbitmq(self,interval,queue,routing_key="datafeeds",rabbit_exchange=""):

        credentials = pika.PlainCredentials(username=self.rabbbitmq_user, password=self.rabbbitmq_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbbitmq_host, credentials=credentials ))
        channel = connection.channel()

        channel.queue_declare(queue=queue)

        channel.basic_publish(exchange=rabbit_exchange, 
                                routing_key=routing_key, 
                                body=interval,
                                properties=pika.BasicProperties(delivery_mode = 2,)
                            )

        print(" [x] Sent NEW DATA IN %s"%interval)
        connection.close()
    def connect(self):
        while True:
            try:
                self.ib.connect('88.99.74.226', 7497, clientId=self.client,timeout=10)
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

    def connect_metatrader(self):
        # connect to MetaTrader 5
        if not mt5.initialize():
            print("metatrader initialize() failed, error code =",mt5.last_error())
            mt5.shutdown()
            
    def database_number(self):
        if self.interval=="1min":
            return 1
        elif self.interval=="5min":
            return 2
        elif self.interval=="10min":
            return 3
        elif self.interval=="15min":
            return 4
        elif self.interval=="30min":
            return 5
        elif self.interval=="1h":
            return 6
        elif self.interval=="2h":
            return 7
        elif self.interval=="4h":
            return 8
        elif self.interval=="8h":
            return 9
        elif self.interval=="1d":
            return 10
        elif self.interval=="1w":
            return 11
        elif self.interval=="1m":
            return 12

    def commudityprice(self,symbol,timeframe,count):
        print('thread symbol %s is start' %symbol)
        try:
            bars =mt5.copy_rates_from_pos(symbol,timeframe,0,count)
        except:
            sleep(randint(1,10))    
            bars =mt5.copy_rates_from_pos(symbol,timeframe,0,count)
            

        
        x=[]
        for bar in bars:
            
            timestamp =datetime.fromtimestamp(bar[0])
            x.insert(0,{
            "date": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(bar[1]),
            "high": float(bar[2]),
            "low" : float(bar[3]),
            "close" : float(bar[4]),
            "volume" : float(bar[5]), 
            })

        y = json.dumps(x)
        while True:
            try:
                r = redis.Redis(host='127.0.0.1', port=6379,password='6$gtA453', db=self.database_number())
                r.set(symbol, y )
                break
            except:
                sleep(0.1)  
        r.connection_pool.disconnect()
        print('thread symbol %s is finish' %symbol)
        #print(y)
        

    def crypto_price(self,symbol, kline_interval,duration,contract):
        try:
            bars =contract.get_historical_klines(symbol, kline_interval,start_str=duration)
        except:
            print("error in symbol data for "+symbol)
            sleep(randint(1,10))    
            bars =contract.get_historical_klines(symbol, kline_interval,start_str=duration)
 
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
        while True:
            try:
                r = redis.Redis(host='127.0.0.1', port=6379,password='6$gtA453', db=self.database_number())
                r.set(symbol, y )
                break
            except:
                sleep(0.1)  
        
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
        while True:
            try:
                r = redis.Redis(host='127.0.0.1', port=6379,password='6$gtA453', db=self.database_number())
                r.set(symbol, y )
                break
            except:
                sleep(0.1)  
        r.connection_pool.disconnect()
        print('thread symbol %s is finish' %symbol)
        #print(y)
        
    def exit(self):
        os.kill(os.getpid(), signal.SIGTERM)
        print('{} Now the system will to sleep for Next Day '.datetime.now(self.tz_NY).strftime("%H:%M:%S")) #this works ok
    
        a = datetime.now(self.tz_NY)
        b = datetime.now(self.tz_NY)+ relativedelta(days=1)
        b = b.strftime('%Y-%m-%d 09:30:00')
        b = datetime.strptime(b, "%Y-%m-%d %H:%M:%S")
        #print(type(b))
        c=(b-a).total_seconds()
        sleep(c)

    def checkTimeSync(self,interval):
        #return False
        minutes=int(datetime.now(self.tz_NY).strftime("%M"))
        sec=int( datetime.now().strftime("%S"))
        if interval=="1min":
            if minutes%1!=0 or sec!=1:
                return True
            else:
                return False
        elif interval=="5min":
            if minutes%5!=0 or sec!=1:
                return True
            else:
                return False
        elif interval=="15min":
            if minutes%15!=0 or sec!=1:
                return True
            else:
                return False
        elif interval=="30min":
            if minutes%30!=0 or sec!=1:
                return True
            else:
                return False
        else:
            if minutes%60!=0 or sec!=1:
                return True
            else:
                return False

    def getSymbols(self,interval):
        #return ["XAUUSD"]
    
        url = 'https://panel.scanical.com/api/get_symbols/'+interval
        try:
            x = requests.get(url)
            return x.json()
        except:
            return []

    def getExchangeSymbols(self,exchnage):
        url = 'https://panel.scanical.com/api/exchangesymbols/'+exchnage
        try:
            x = requests.get(url)
            return x.json()
        except:
            return []

    def getContract(self,symbol):
        
        
        if symbol in self.primary_nasdaq:
            
            contract = Stock(symbol.lower(), 'SMART', 'USD', primaryExchange='NASDAQ')
        elif symbol in self.primary_nyse:
            
            contract = Stock(symbol.lower(), 'SMART', 'USD', primaryExchange='NYSE')

        elif symbol in self.forex_symbol:   

            contract = Forex(symbol.lower())

        elif symbol in self.crypto_symbol:
            #print('crypto load')
            contract = self.crypto_client 
            
        elif symbol in self.commodity_symbol:
            
            contract = ""          
        else:
            
            contract = Stock(symbol.lower(), 'SMART', 'USD')

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
    def get_meta_interval(self,interval):

        if interval=='1min':
            return mt5.TIMEFRAME_M1
        elif interval=='5min':
            return mt5.TIMEFRAME_M5
        elif interval=='10min':
            return mt5.TIMEFRAME_M10
        elif interval=='15min':
            return mt5.TIMEFRAME_M15
        elif interval=='30min':
            return mt5.TIMEFRAME_M30
        elif interval=='1h':
            return mt5.TIMEFRAME_H1
        elif interval=='2h':
            return mt5.TIMEFRAME_H2
        elif interval=='4h':
            return mt5.TIMEFRAME_H4
        elif interval=='6h':
            return mt5.TIMEFRAME_H6
        elif interval=='8h':
            return mt5.TIMEFRAME_H8
        elif interval=='12h':
            return mt5.TIMEFRAME_H12
        elif interval=='1d':
            return mt5.TIMEFRAME_D1
        elif interval=='1w':
            return mt5.TIMEFRAME_W1
        elif interval=='1m':
            return mt5.TIMEFRAME_MN1

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
            return "3 D"
        elif interval=='5min':
            return "3 D"
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
    def get_meta_duration(self,interval):
        
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
    def update_bar(self):
        claster=self.getSymbols(self.interval)
        kline_interval=self.get_kline_interval(self.interval)
        barsize= self.get_bar_size(self.interval)
        duration= self.get_kline_duration(self.interval)
        ibDuration=self.get_ib_duration(self.interval)
        #metaDuration =self.get_meta_duration(self.interval)
        meta_interval=self.get_meta_interval(self.interval)
        
        # try:
        for symbol in claster:
            contract=self.getContract(symbol)

            if symbol in self.crypto_symbol:
                try:
                    #bars =contract.get_historical_klines(symbol, kline_interval,start_str=duration)
                    #_thread.start_new_thread(self.crypto_price,(bars,symbol,))
                    #_thread.start_new_thread(self.crypto_price,(symbol, kline_interval,duration,contract,))
                    t= threading.Thread(target=self.crypto_price,args=(symbol, kline_interval,duration,contract,))
                    self.threads.append(t)
                    t.start()
                
                except Exception as e:
                    print("try to connect binance again:",e)
                    self.connect_binance()
            elif symbol in self.commodity_symbol:
                try:
                    #self.commudityprice(symbol,meta_interval,200)
                    t= threading.Thread(target=self.commudityprice,args=(symbol,meta_interval,202,))
                    self.threads.append(t)
                    t.start()
                
                except Exception as e:
                    print("try to connect meta again:",e)
                    print("error code =",mt5.last_error())
                    self.connect_metatrader()
            elif symbol in self.forex_symbol:
                candelShow='MIDPOINT'
                try:
                    bars =self.ib.reqHistoricalData(
                        contract, endDateTime='', durationStr=ibDuration,
                        barSizeSetting=barsize, whatToShow=candelShow, useRTH=True)
                    
                    t= threading.Thread(target=self.stockprice,args=(bars,symbol,))
                    self.threads.append(t)
                    t.start()
                    #_thread.start_new_thread(self.stockprice,(bars,symbol,))
                except:
                    print("try to connect again")
                    self.connect()
            else:
                candelShow='TRADES'
                try:
                    bars =self.ib.reqHistoricalData(
                        contract, endDateTime='', durationStr=ibDuration,
                        barSizeSetting=barsize, whatToShow=candelShow, useRTH=True)
                    
                    t= threading.Thread(target=self.stockprice,args=(bars,symbol,))
                    self.threads.append(t)
                    t.start()
                except:
                    try:
                        print("try to connect again")
                        self.connect()   
                    except Exception as e:
                        print("ib have problem with connection")
                        print(e)          


        flag =True

        while (flag):
            sleep(0.5)
            flag = self.isTreadAlive()
        if len(claster)>0:
            self.connect_rabbitmq(interval=self.interval,queue="datafeeds")  
            
                  
    def main(self,function):
        self.jobs.insert(0,function)
        pass        
        # except:
        #     try:
        #         print("try to connect again")
        #         self.ib.connect('127.0.0.1', 7497, clientId=self.client,timeout=10)
        #     except:
        #         print("ib have problem with connection")


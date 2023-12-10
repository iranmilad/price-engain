from  PriceEngine import PriceSeries
import time
import pytz
from datetime import datetime

tz_NY = pytz.timezone('America/New_York') 
datetime_NY = datetime.now(tz_NY)



client=57
interval="2h"
sleep_loop_time=2*60*60

Engine=PriceSeries(client,interval)
Engine.update_bar()

while Engine.checkTimeSync(interval):
    
    time.sleep(1)
    print("Timing synchronization at "+datetime_NY.strftime("%H:%M")+":"+datetime.now().strftime("%S"))


print("End of synchronization")




while True:
    
    start_time = time.time()

    Engine.update_bar()

    print("--- %s seconds --- at %s" % (time.time() - start_time,datetime.now().strftime("%H:%M:%S")))
    
    if sleep_loop_time-(time.time() - start_time)>0:
        time.sleep(sleep_loop_time-(time.time() - start_time))
ib.disconnect()

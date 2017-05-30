from datetime import timedelta, date
from time import sleep
import time
import extractor_main
import settings

if __name__ == '__main__':
    #start_date = date(2016, 11, 1)
    #end_date = date(2016, 12, 31)
    start_date = date(2017, 3, 29)
    end_date = date(2017, 5, 30)
    
    d = start_date    
    delta = timedelta(days=1)
    while d <= end_date:
        extractor_main.main({'requestedDate' : d.strftime("%d/%m/%Y")})
        sleep(settings.REQ_INTERVAL)
        d += delta





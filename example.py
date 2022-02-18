import logging
import time
import tracemalloc
import aiohttp

import db_interact as db
import helpers as hlp
import historical_data as ghd  #  aiohttp should be imported before mysql.connector, if not it causes weird errors

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    # In order to start collecting candles, we need to create a database first. Run this block of code only once.
    conn_creds = {  # You need to setup MySQL first, then you will know your host, user and password.
        'host': 'localhost', #  replace with your host name
        'user': 'user', # this too
        'password': 'password' #  and this
    }
    conn_db = db.ConnectionDB(**conn_creds)
    conn_db.connect()
    conn_db.create_database('DB_name') #  Change to a meaningful name
    conn_db.close_connection()


    # Ok, database created, now let's get some candles
    start_perf = time.perf_counter()
    tracemalloc.start()

    conn_creds = {
        'host': 'localhost', #  replace with your host name
        'user': 'user', # this too
        'password': 'password', #  and this
        'database': 'DB_name'
    }

    candle_getter = ghd.data_manager('BTCUSDT', '1d')
    candle_getter.set_database_credentials(**conn_creds)

    start = hlp.date_to_milliseconds('25 Nov 2018')
    end = hlp.date_to_milliseconds('29 Nov 2018')

    fetch = candle_getter.get_candles(start_ts=start, end_ts=end, delete_existing_table=False)

    for candle in fetch:
        print(f'open time: {hlp.ts_to_date(candle[0])};'
              f'close time: {hlp.ts_to_date(candle[6])}; '
              f'len: {round((candle[6] - candle[0]) / (1000 * 60 * 60 * 24))} day; ')

    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
    tracemalloc.stop()
    print(f'it took {time.perf_counter() - start_perf} seconds')

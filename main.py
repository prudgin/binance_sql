# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import logging
import sys
import gc
import tracemalloc
import time
import historical_data as ghd  # aiohttp should be imported before mysql.connector
import db_interact as db
import helpers as hlp

import spooky

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    conn_creds = {
        'host': spooky.creds['host'],
        'user': spooky.creds['user'],
        'password': spooky.creds['password'],
        'database': spooky.creds['database']
    }
    conn_db = db.ConnectionDB(**conn_creds)

    # print(conn)
    conn_db.connect()

    # print(conn_db.list_databases())
    #conn_db.table_delete('BTCUSDT1mHist')

    conn_db.close_connection()

    # 1502668800000 default start
    # end_ts = int(time.time() * 1000) = now
    start = time.perf_counter()

    # https://stackoverflow.com/questions/1316767/how-can-i-explicitly-free-memory-in-python

    tracemalloc.start()
    candle_getter = ghd.data_download_store('BTCUSDT', '1m')
    fetch = candle_getter.get_candles(start_ts=1543104000000,
                                    end_ts=1543104000000 - 1 + 60000 * 60 * 1000)
    print(fetch)
    for candle in fetch:
        print(f'open time: {hlp.ts_to_date(candle[0])};'
              f'close time: {hlp.ts_to_date(candle[6])}; '
              f'len: {round((candle[6] - candle[0]) / (1000 * 60 * 60 * 24))}; ')

    # del fetched_candles
    # gc.collect()

    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
    tracemalloc.stop()

    print(f'it took {time.perf_counter() - start}')



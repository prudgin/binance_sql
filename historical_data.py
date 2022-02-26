import asyncio
import logging
import sys
import time
from decimal import Decimal
import aiohttp

from binance import exceptions as pybin_exceptions
from binance.client import AsyncClient, Client

import binance_sql.db_interact as db
import binance_sql.exceptions as exceptions
import spooky
import binance_sql.helpers as helpers

"""
This module downloads historical data from binance via API, writes it to the MySQL database, then returns data.
The downloading occurs concurrently, using asyncio.
To get data one should run get_candles method, specifying start and end of period of interest.
The central idea is to first check if requested data is in the database. But data in DB can be fragmented, e.g.
we can have data from 1 September to 5 Sep, then from 10 Sep to 15 Sep and so on.
In order to deal with gaps, we search for them with a tricky SQL request. Then, we iterate over gaps and download data.
After all gaps have been filled, data is fetched from the database and returned to the user.
"""

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.WARNING,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)

logger = logging.getLogger(__name__)


class data_manager:
    """
    This stores data, downloads data and returns data upon request.
    """

    def __init__(self,
                 symbol: str,  # BTCUSDT for example
                 interval: str,  # 1d or 1h or else
                 limit=500,  # number of candles downloaded per API request
                 max_coroutines=10):  # max simultaniously running concurrent workers, who download data

        self.interval = interval
        self.interval_ms = helpers.interval_to_milliseconds(interval)
        if self.interval_ms is None:
            raise ValueError('Bad interval. Shold be string like 1h, 1m, 1d and so on.')

        self.symbol = symbol

        # table names for each symbol and interval are created this way:
        self.table_name = f'{symbol}{interval}Hist'

        self.limit = limit
        self.max_coroutines = max_coroutines
        self.exchange_client = None

        self.rounded_sum = 0
        self.missing_sum = 0
        self.candles_loaded = 0

    def set_database_credentials(self, host: str, user: str, password: str, database: str):
        self.conn_db = db.ConnectionDB(host=host, user=user, password=password, database=database)

    def cleanup(self):
        if self.conn_db:
            self.conn_db.close_connection()
        if self.exchange_client:
            async def close_client():
                await self.exchange_client.close_connection()

            asyncio.run(close_client())

    def prepare_initial_conditions(self, start_ts: int, end_ts: int, delete_existing_table: bool):
        """
        adjust start and end time and create table in database if abscent
        """
        if not hasattr(self, 'conn_db'):
            raise AttributeError('Run set_database_credentials() first')

        print(f'requested candles from {start_ts} {helpers.ts_to_date(start_ts)} to '
              f'{helpers.ts_to_date(end_ts)} {end_ts}')

        # Requested end is in future
        if end_ts > int(time.time() * 1000):
            end_ts = int(time.time() * 1000)
            logger.warning(f'end date is in future, adjusting to now: {helpers.ts_to_date(end_ts)}')

        # if end_ts == start_ts, still one candle with open_time = start_ts will be returned
        if start_ts > end_ts:
            logger.warning('start date > end date, abort')
            self.cleanup()
            return None

        # earliest candle available from exchange
        earliest_timestamp = Client()._get_earliest_valid_timestamp(symbol=self.symbol, interval=self.interval)
        self.start_ts = max(start_ts, earliest_timestamp)
        self.end_ts = min(end_ts, int(time.time() * 1000))

        # if start_ts = 167000000001 round it up to next candle start, end_ts is rounded down
        self.start_ts, self.end_ts = helpers.round_timings(self.start_ts, self.end_ts, self.interval_ms)

        print(f'going to fetch candles from {self.start_ts} {helpers.ts_to_date(self.start_ts)} to '
              f'{helpers.ts_to_date(self.end_ts)} {self.end_ts}')

        # connecting to database
        if not self.conn_db.connect():
            self.cleanup()
            return None

        # Table present and delete flag is on
        if self.conn_db.table_in_db(self.table_name) and delete_existing_table:
            self.conn_db.table_delete(self.table_name)

        # Try to create table. Will do nothing and return True if table already there.
        if not self.conn_db.table_create(self.table_name):
            self.cleanup()
            return None

    def get_gaps(self, time_col_name='open_time'):
        """
        check if requested data is present in database
        search for gaps in interval from start_ts to end_ts, gap = absence of some data in database
        gaps is a list of tuples [(gap1_start, gap1_end), (gap2_start, gap2_end), ...]
        returns None in case of error
        """
        logger.debug('start get_gaps')

        gaps = self.conn_db.get_missing(self.table_name, self.interval_ms, self.start_ts, self.end_ts)

        if not gaps and gaps is not None:  # if we got an empty list but not a None
            print('all requested data already present in database, no gaps found')
        logger.debug('end get_gaps')
        return gaps

    def get_candles(self, start_ts: int, end_ts: int,
                    reversed_order=True,
                    delete_existing_table=False) -> list:
        """
        Returns candles with open_time fall in: start_ts <= open_time <= end_ts.
        If reversed_order = True, candles are sorted from newer to older, the list be [new, old].
        It is needed to pop older first from list.
        Returns a list of (open_time, open, high, low, close, volume, close_time,
        quote_vol, num_trades, buy_base_vol, buy_quote_vol).
        Exceptions scheme:
        this function raises no exceptions, tries to catch all, and returns None if caught an exception.

        :param start_ts: starting timestamp in milliseconds
        :param end_ts: end timestamp in milliseconds
        :param reversed_order: True if you need oldest candles first
        :param delete_existing_table: or not?
        :return: list of tuples
        """

        fetch = None  # Result we are going to return
        if not self.prepare_initial_conditions(start_ts, end_ts, delete_existing_table):
            logger.error('failed to initialise, returning None')
            self.cleanup()
            return None

        gaps = self.get_gaps()
        if gaps is None:  # get_gaps function encountered an error
            logger.error('get_gaps function returned None')
            self.cleanup()
            return None

        #  Get when last entry was added to the table, need this for printing report of how many candles were written.
        #  In case of error returns 0.
        last_entry_id = self.conn_db.get_latest_id(self.table_name)

        if gaps:  #  If there are gaps in a period of interest.
            print(' found gaps:')
            for gap in gaps:
                print(f' {gap[0]} {helpers.ts_to_date(gap[0])} - {helpers.ts_to_date(gap[1])} {gap[1]}')

            for gap in gaps:
                print(f'  loading gap from exchange: {gap[0]} {helpers.ts_to_date(gap[0])} - '
                      f'{helpers.ts_to_date(gap[1])} {gap[1]}')

                # get candles covering gap from exchange and write them in the database
                if not asyncio.run(self.get_write_candles(gap[0], gap[1])):
                    logger.error('get_write_candles failed.')
                    self.cleanup()
                    return None

        _, _, count_written = self.conn_db.get_start_end_later_than(self.table_name, last_entry_id, only_count=True)
        if count_written:
            print(f'wrote {count_written} candles to db')

        # Ok, we tried to get data from exchange, now just fetch from database:
        logger.debug('reading from db: conn_db.read()')
        fetch = self.conn_db.read(self.table_name, self.start_ts, self.end_ts, reversed_order=reversed_order)
        if fetch:
            print(f'fetched {len(fetch)} candles from the database')
        self.cleanup()
        return fetch

    async def get_write_candles(self, gap_start, gap_end):
        #  Return None if fails, else return True
        #  Create the Binance client, no need for api key.
        try:
            self.exchange_client = await AsyncClient.create(requests_params={"timeout": 60})
        except aiohttp.client_exceptions.ClientConnectorCertificateError as err:
            logger.error(f'ClientConnectorCertificateError: {err}, try importing aiohttp prior to everything else')
            self.cleanup()
            return None

        # Get when last entry was added to the table, need this to print out a final report.
        last_entry_id = self.conn_db.get_latest_id(self.table_name)

        # Break the timeline of interest into periods, each period will get it's own concurrent worker.
        periods = self.get_limit_intervals(gap_start, gap_end)

        #  If we have too many concurrent requests at the same time, some of them get timeouted.
        #  In order to handle theese, we run timeouted requests again. Unless there is decrease in their number.
        #  Actually, this is an overkill, because if we don't go with more then 50 concurrent tasks at a time,
        #  it is very unlikely to get a timeout error.
        #  I just wanted to be shure I squeezed every little piece of data out of the exchange.
        i = 1
        while True:
            # Gather_write_candles packs all async tasks together.
            # Each task downloads and writes to DB some data (one period, each obtained by get_limit_intervals).
            timeout_gaps = await self.gather_write_candles(periods)
            if (
                    not timeout_gaps or  # Stop cycle if we recieve no timeout errors from exchange.
                    len(timeout_gaps) == len(periods) or  # or if recieve same number of timeouts as the last iteration.
                    i > 10  # I am afraid of getting stuck in infinite loops.
            ): break
            print(f'   Got timeouts from exchange, going to iterate one more time. Iteration N {i}.\n'
                  f'   Consider lowering max_coroutines parameter in get_candles_from_db function.')
            i += 1
            periods = timeout_gaps

        print('   gap summary:')
        first_written, last_written, count_written = self.conn_db.get_start_end_later_than(self.table_name,
                                                                                           last_entry_id)
        if first_written and last_written:
            print(f'    wrote to db {count_written} candles starting from '
                  f'{first_written} {helpers.ts_to_date(first_written)} to '
                  f'{helpers.ts_to_date(last_written)} {last_written}')
        else:
            print(f'   wrote {count_written} candles')

        await self.exchange_client.close_connection()

        # send OK signal to get_candles() method
        return True

    def get_limit_intervals(self, gap_start, gap_end):
        """
        Splits time from gap_start to gap_end into intervals, each interval is for one api request,
        like[[start, end], [start2, end2], ...], end - start = interval_ms*limit.
        limit = number of candles fetched per 1 API request
        interval_ms = candle "width"
        """
        intervals = []
        int_start = gap_start
        while int_start <= gap_end:
            int_end = int_start + self.interval_ms * (self.limit - 1) - 1
            if int_end > gap_end:
                int_end = gap_end
            intervals.append([int_start, int_end])
            int_start = int_end + 1
        return intervals

    async def gather_write_candles(self, periods: list):
        """
        Initialises concurrent tasks, each with it's own time period, then packs them all together.
        :param periods: list of periods
        :return: faulty time gaps that were not filled with data due to some errors (exchange timeout)
        """
        sem = asyncio.Semaphore(self.max_coroutines)
        async def sem_task(task):
            async with sem:
                return await task

        tasks = [
            sem_task(
                # write_candles calls candle download function, then write downloaded candles to DB
                self.write_candles(period[0], period[1])
            )
            for period in periods
        ]

        gathered_result = await asyncio.gather(*tasks)
        # some bunches don't get downloaded due totimeout error, this results in gaps in data
        timeout_gaps = [result[1] for result in gathered_result if result[1]]

        good_bunches = sum([1 for i in gathered_result if i[0]])
        error_bunches = sum([1 for i in gathered_result if i[0] is None])
        count_timeout_gaps = sum([1 for i in gathered_result if i[1]])
        print(f'\n   total bunches: {len(gathered_result)}, '
              f'errorless bunches: {good_bunches}, bunches with errors: {error_bunches}, '
              f'timeout gaps: {count_timeout_gaps}')

        return timeout_gaps

    async def write_candles(self, period_start: int, period_end: int):
        """
        Load candles from exchange, then writes to database.
        :param period_start: timestamp in ms
        :param period_end: timestamp in ms
        :return: Nothing. It writes to database.
        """
        temp_data, timeout_gap = await self.download_candles(period_start, period_end)

        try:
            api_weight = self.exchange_client.response.headers['x-mbx-used-weight-1m']
        except AttributeError as err:
            logger.error(f'getting api weight, got: {err}')
            api_weight = 599
        except KeyError as err:
            api_weight = 599
            logger.error(f'getting api weight, probably got response != 200. Error is: {err}')

        if int(api_weight) > 600:
            sleep_time = int(10 * int(api_weight) ** 3 / 1200 ** 3)
            logger.warning(f'reaching high api load, current api_weight:'
                           f' {api_weight}, max = 1200, sleep for {sleep_time} sec')
            # not awaitable, because we need to block everything in order to lower api weight
            time.sleep(sleep_time)

        if temp_data is None or not len(temp_data):
            return (None, timeout_gap)
        else:
            # write candles to DB
            if self.conn_db.write(temp_data, self.table_name):
                return (True, timeout_gap)
            else:
                return (None, timeout_gap)

    async def download_candles(self, period_start: int, period_end: int):
        """
        Download candles from binance.
        Candles with open_time >= start_ts are included.
        Candles with open_time <= end_ts are included too.
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_klines
        Binance API returns the following:
        [
            [
                1499040000000,      # Open time
                "0.01634790",       # Open
                "0.80000000",       # High
                "0.01575800",       # Low
                "0.01577100",       # Close
                "148976.11427815",  # Volume
                1499644799999,      # Close time
                "2434.19055334",    # Quote asset volume
                308,                # Number of trades
                "1756.87402397",    # Taker buy base asset volume
                "28.46694368",      # Taker buy quote asset volume
                "17928899.62484339" # Can be ignored
            ]
        ]
        """
        temp_data = None
        rounded_count = 0
        timeout_gap = []

        try:
            # if start_ts = end_ts, this will return one candle with open_time = start_ts
            temp_data = await self.exchange_client.get_klines(
                symbol=self.symbol,
                interval=self.interval,
                limit=self.limit,
                startTime=period_start,
                endTime=period_end
            )
        except asyncio.TimeoutError as err:
            logger.warning(f'get_candles, client.get_klines: asyncio.TimeoutError; {err}')
            timeout_gap = [period_start, period_start]

        except pybin_exceptions.BinanceRequestException as err:
            logger.error(f'binance returned a non-json response, {err}')

        except pybin_exceptions.BinanceAPIException as err:
            logger.error(f'API call error, probably bad API request, details below:\n'
                         f'     response status code: {err.status_code}\n'
                         f'     response object: {err.response}\n'
                         f'     Binance error code: {err.code}\n'
                         f'     Binance error message: {err.message}\n'
                         f'     request object if available: {err.request}\n'
                         )

        except Exception as err:
            logger.error(f'get_candles: unknown error: {err}')

        if temp_data is not None:

            temp_data, self.rounded_count = helpers.round_data(temp_data, self.interval_ms)
            self.rounded_sum += self.rounded_count

            # if sent correct request without raising any errors, check for gaps in data
            # if there are any gaps, write expected candles with -1 in all fields except for open_time and close_time
            response_status = self.exchange_client.response.status

            if response_status == 200:
                temp_gaps = helpers.get_data_gaps(temp_data, period_start, period_end, self.interval_ms)
                shift = 0
                for gap in temp_gaps:
                    insert_index = gap[2] + shift
                    generated = helpers.generate_data(gap[0], gap[1], self.interval_ms)
                    self.missing_sum += len(generated)
                    temp_data[insert_index:insert_index] = generated
                    shift += len(generated)

            #  insert "time_loaded" column in temp_data list of lists, set it to now()
            time_loaded = int(time.time() * 1000)
            temp_data = [k + [time_loaded] for k in temp_data]

            last_in_data = temp_data[-1][0]
            self.candles_loaded += len(temp_data)

            str1 = "\r   last candle:{0}, candles loaded:{1}, rounded:{2}, missing:{3}, tasks remain:{4}".format(
                helpers.ts_to_date(last_in_data),
                self.candles_loaded,
                self.rounded_sum,
                self.missing_sum,
                len(asyncio.all_tasks()) - 2
            )
            sys.stdout.write(str1)


        else:
            logger.error(f'could not load data')

        return (temp_data, timeout_gap)


if __name__ == '__main__':
    pass

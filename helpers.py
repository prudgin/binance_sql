import logging
from datetime import datetime
import dateparser
import pytz
import time
import functools
import mplfinance as mpf
import pandas as pd

logger = logging.getLogger(__name__)


def ts_to_date(ts):
    # transform a timestamp in milliseconds into a human readable date
    return datetime.utcfromtimestamp(ts / 1000).strftime("%d-%b-%Y %H:%M:%S")


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    1 jan 1970 was thursday, I found this out too late, so the program don't accept 1w interwals
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d
    :type interval: str
    :return:
         None if unit not one of m, h, d
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            logger.error(f'interval_to_milliseconds got invalid interval[:-1],'
                         f' was expecting int, got {interval[:-1]}')
    else:
        logger.error(f'interval_to_milliseconds got invalid interval unit {unit},'
                     f'valid interval units are: m, h, d')
    return ms

def round_timings(start_ts, end_ts, interval_ms):
    # get start and end timings rounded to interval_ms
    # round up to the next candle open_time, can use math.ceil here
    # because if we get start_ts = 167000000001 we don't want to start from 167000000000,
    # but from the next following candle after 167000000001. end_ts is rounded down
    rdd_start = interval_ms * (start_ts // interval_ms + (start_ts % interval_ms > 0))
    rdd_end = interval_ms * (end_ts // interval_ms)
    return rdd_start, rdd_end


def round_data(temp_data, interval_ms):
    # some candles have open_time not multiple of candle width, need to correct them
    # for example, while most 1 minute candles have timings like 15:00:00, some have 15:00:14
    # i[0] = open_time, i[6] = close_time
    # add [1] at the beginnig of the list if correction is going to happen
    # else add [0], in order to count corrections
    # the question is - do I really need to correct those?
    rounded_data = [
        [1] + [rdd] + i[1:6] + [rdd - i[0] + i[6]] + i[7:]
        if (rdd := interval_ms * round(i[0] / interval_ms)) != i[0]
        else
        [0] + i
        for i in temp_data]
    rounded_count = sum([i[0] for i in rounded_data])
    rounded_data = [i[1:] for i in rounded_data]
    return rounded_data, rounded_count


def generate_data(start_ts, end_ts, interval_ms):
    rdd_start, rdd_end = round_timings(start_ts, end_ts, interval_ms)
    data = [
        [
            open_time,  # open time
            -1,  # open
            -1,  # high
            -1,  # low
            -1,  # close
            -1,  # volume
            open_time + interval_ms - 1,  # close_time
            -1,  # quote_vol
            -1,  # num_trades
            -1,  # buy base vol
            -1,  # buy quote vol
            -1  # ignored
        ] for open_time in range(rdd_start, rdd_end + interval_ms, interval_ms)
    ]
    return data


def get_data_gaps(temp_data, start_ts, end_ts, interval_ms):
    # presume temp_data list is ordered by opening time
    rdd_start, rdd_end = round_timings(start_ts, end_ts, interval_ms)

    if not len(temp_data):
        gaps_list = [[rdd_start, rdd_end, 0]]

    else:
        gaps_list = [
            [expected, temp_data[i][0] - interval_ms, i]
            for i in range(1, len(temp_data))
            if
            temp_data[i][0] >= (expected := temp_data[i - 1][0] + interval_ms) + interval_ms and
            rdd_end >= expected
        ]
        if temp_data[0][0] - rdd_start >= interval_ms:
            gaps_list.insert(0, [rdd_start, temp_data[0][0] - interval_ms, 0])

        if temp_data[-1][0] + interval_ms <= rdd_end:
            gaps_list.append([temp_data[-1][0] + interval_ms, rdd_end, len(temp_data)])

        if len(gaps_list) and gaps_list[-1][1] > rdd_end:
            gaps_list[-1][1] = rdd_end

    return gaps_list


if __name__ == '__main__':
    pass

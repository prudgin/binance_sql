
def round_timings(start_ts, end_ts, interval_ms):
    """
    Get start and end timings rounded to interval_ms.
    Round up to the next candle open_time, can use math.ceil here,
    because if we get start_ts = 167000000001 we don't want to start from 167000000000,
    but from the next following candle after 167000000001. end_ts is rounded down.
    """
    rdd_start = interval_ms * (start_ts // interval_ms + (start_ts % interval_ms > 0))
    rdd_end = interval_ms * (end_ts // interval_ms)
    return rdd_start, rdd_end
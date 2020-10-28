#!/usr/bin/env python3

import time

import arrow

# import click
import datetime
from influxdb import InfluxDBClient
from influxdb import SeriesHelper
from influxdb import DataFrameClient
from dotenv import load_dotenv
from pathlib import Path
import json
import pandas as pd
from flatten_dict import flatten
import schedule
import functools

from utils.parsers import PARSER_KEY_TO_DICT

env_path = Path(".") / "secrets.env"
load_dotenv(dotenv_path=env_path)

verbose = False # Print query results?

# Decorator for supressing errors during runtime
def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback

                print(datetime.datetime.now().isoformat(), traceback.format_exc())
                if cancel_on_failure:
                    return schedule.CancelJob

        return wrapper

    return catch_exceptions_decorator


@catch_exceptions(cancel_on_failure=False)
def parse_to_influx(zone, data_type, target_datetime):
    """

    Parameters
    ----------
    zone: a two letter zone from the map
    data_type: in ['production', 'exchangeForecast', 'production', 'exchange',
      'price', 'consumption', 'generationForecast', 'consumptionForecast']
    target_datetime: string parseable by arrow, such as 2018-05-30 15:00

    Examples
    -------
    >>> python parse_to_influx.py NO-NO3-\>SE exchange
    parser result:
    {'netFlow': -51.6563, 'datetime': datetime.datetime(2018, 7, 3, 14, 38, tzinfo=tzutc()), 'source': 'driftsdata.stattnet.no', 'sortedZoneKeys': 'NO-NO3->SE'}
    ---------------------
    took 0.09s
    min returned datetime: 2018-07-03 14:38:00+00:00
    max returned datetime: 2018-07-03T14:38:00+00:00 UTC  -- OK, <2h from now :) (now=2018-07-03T14:39:16.274194+00:00 UTC)
    >>> python parse_to_influx.py FR production
    parser result:
    [... long stuff ...]
    ---------------------
    took 5.38s
    min returned datetime: 2018-07-02 00:00:00+02:00
    max returned datetime: 2018-07-03T14:30:00+00:00 UTC  -- OK, <2h from now :) (now=2018-07-03T14:43:35.501375+00:00 UTC)
    """
    if target_datetime:
        target_datetime = arrow.get(target_datetime).datetime
    start = time.time()

    parser = PARSER_KEY_TO_DICT[data_type][zone]
    if data_type in ["exchange", "exchangeForecast"]:
        args = zone.split("->")
    else:
        args = [zone]
    res = parser(*args, target_datetime=target_datetime)

    if not res:
        print("Error: parser returned nothing ({})".format(res))
        return

    elapsed_time = time.time() - start
    if isinstance(res, (list, tuple)):
        res_list = list(res)
    else:
        res_list = [res]

    try:
        dts = [e["datetime"] for e in res_list]
    except:
        print(
            "Parser output lacks `datetime` key for at least some of the "
            "ouput. Full ouput: \n\n{}\n".format(res)
        )
        return

    assert all(
        [type(e["datetime"]) is datetime.datetime for e in res_list]
    ), "Datetimes must be returned as native datetime.datetime objects"

    reduced_list = []
    for var in res_list:
        reduced_list.append(flatten(var, reducer="underscore"))

    df = pd.DataFrame(reduced_list)

    df = df.set_index("datetime")
    if "exchange" in data_type:
        client.write_points(df, data_type + zone, protocol="json")
    else:
        client.write_points(df, data_type, protocol="json")

    last_dt = arrow.get(max(dts)).to("UTC")
    first_dt = arrow.get(min(dts)).to("UTC")
    max_dt_warning = ""
    if not target_datetime:
        max_dt_warning = (
            " :( >2h from now !!!"
            if (arrow.utcnow() - last_dt).total_seconds() > 2 * 3600
            else " -- OK, <2h from now :) (now={} UTC)".format(arrow.utcnow())
        )

    global verbose
    if verbose:
        print(
            "\n".join(
                [
                    "parser result:",
                    res.__str__(),
                    "---------------------",
                    "took {:.2f}s".format(elapsed_time),
                    "min returned datetime: {} UTC".format(first_dt),
                    "max returned datetime: {} UTC {}".format(last_dt, max_dt_warning),
                ]
            )
        )
    else:
        print(datetime.datetime.now().isoformat(),'Fetched', data_type, zone)

if __name__ == "__main__":
    # Connect to database
    client = DataFrameClient(host="localhost", port=8086, database="influx")

    # Schedule fetch of production and generation values with forecast
    schedule.every(12).hours.do(
        parse_to_influx, zone="AT", data_type="production", target_datetime=None
    )
    schedule.every(12).hours.do(
        parse_to_influx, zone="AT", data_type="generationForecast", target_datetime=None
    )
    schedule.every(12).hours.do(
        parse_to_influx,
        zone="AT",
        data_type="consumptionForecast",
        target_datetime=None,
    )

    # Schedule fetch of energy exchange and forecast
    exchange_zones = ["AT->CH", "AT->CZ", "AT->DE", "AT->HU", "AT->IT-NO", "AT->SI"]
    for zone_name in exchange_zones:
        schedule.every(12).hours.do(
            parse_to_influx, zone=zone_name, data_type="exchange", target_datetime=None
        )
        schedule.every(12).hours.do(
            parse_to_influx,
            zone=zone_name,
            data_type="exchangeForecast",
            target_datetime=None,
        )

    # Fetch price and consumption regularly
    schedule.every(15).minutes.do(
        parse_to_influx, zone="AT", data_type="price", target_datetime=None
    )
    schedule.every(15).minutes.do(
        parse_to_influx, zone="AT", data_type="consumption", target_datetime=None
    )

    # Initial run on script startup
    schedule.run_all()

    # Continous queries during execution
    while True:
        schedule.run_pending()
        time.sleep(1)

import requests
import pendulum

import pandas as pd

from typing import List
from pandas import DataFrame
from functools import reduce
from operator import add
from timezonefinder import TimezoneFinder


API_KEY = "f0ec0abb9b8143349f1130303211901"
WWO_URI = f"http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key={API_KEY}&q={{lat}},{{lon}}&format=json&extra=localObsTime,utcDateTime,isDayTime&date={{start}}&enddate={{end}}&tp=1"


def get_raw_weather_json(*, lat: float, lon: float, start: str, end: str) -> dict:
    """
    Pulls hourly weather data from API for single location between start and end dates.

    NB: Will return max 35 days # TODO add check for this?

    Args:
        lat (float): latitude of location
        lon (float): longitude of location
        start (str): start date in the form YYYY-MM-DD
        end (str): end date in the form YYYY-MM-DD

    Returns:
        dict: raw json response
    """
    uri = WWO_URI.format(lat=lat, lon=lon, start=start, end=end)
    r = requests.get(uri)

    return r.json()


def process_raw_weather_json(
    *, raw_json: dict, lat: float, lon: float, id: str
) -> DataFrame:
    """
    Processes daily and hourly attributes and returns joined dataframe
    with UTC/local timestamps added.

    Args:
        raw_json (dict): data returned from wwo api
        lat (float): latitude of location (for calculation of local timezone)
        lon (float): longitude of location (for calculation of local timezone)

    Returns:
        DataFrame: processed result dataframe with hourly data

    """
    json = raw_json["data"]["weather"]

    daily_data = []
    hourly_data = []
    for day in json:
        daily_data.append(process_daily_data(raw_daily_json=day))
        hourly_data.append(process_day_of_hourly_data(raw_daily_json=day))

    # turn list of lists into list via concat
    hourly_data = reduce(add, hourly_data)

    hourly_data = pd.DataFrame.from_dict(hourly_data)
    daily_data = pd.DataFrame.from_dict(daily_data)

    tf = TimezoneFinder()
    tz = tf.timezone_at(lng=lon, lat=lat)

    df = pd.merge(hourly_data, daily_data, how="left", on="date")
    df = df.assign(
        utc_datetime=df.apply(lambda x: parse_datetime(x.UTCdate, x.UTCtime), axis=1),
        local_datetime=df.apply(lambda x: parse_datetime(x.date, x.time, tz), axis=1),
        id=id,
    )

    df = df.drop(["date", "time", "UTCdate", "UTCtime"], axis=1)

    return df


def process_day_of_hourly_data(*, raw_daily_json: dict) -> List[dict]:
    """
    Extracts hourly data from each day in the raw json response

    Args:
        raw_daily_json (dict):  data returned from wwo api

    Returns:
        dict:
    """
    # make a copy
    json = dict(raw_daily_json)

    date = json["date"]  # need to join to the daily attributes
    hourly = json["hourly"]

    hourly = [
        process_hourly_data(raw_hourly_json=raw_hourly_json, date=date)
        for raw_hourly_json in hourly
    ]

    return hourly


def process_hourly_data(*, raw_hourly_json: dict, date: str) -> dict:
    """[summary]

    Args:
        raw_hourly_json (dict): [description]
        date (str): [description]

    Returns:
        dict: [description]
    """
    json = dict(raw_hourly_json)

    del json["weatherIconUrl"]  # dont need!

    json["weatherDesc"] = json["weatherDesc"][0]["value"]
    json["date"] = date

    return json


def process_daily_data(*, raw_daily_json: dict) -> dict:
    """[summary]

    Args:
        raw_daily_json (dict): [description]

    Returns:
        dict: [description]
    """
    # make a copy
    json = dict(raw_daily_json)

    # flatten the astro data
    astro = json["astronomy"][0]
    json.update(astro)

    # remove unneeded keys
    del json["astronomy"]
    del json["hourly"]

    return json


def parse_datetime(date: str, hour: str, tz: str = "UTC") -> str:
    """[summary]

    Args:
        date (str): [description]
        hour (str): [description]
        tz (str, optional): [description]. Defaults to None.

    Returns:
        str: [description]
    """
    datetime = pendulum.from_format(
        f"{date} {int(int(hour) / 100)}", "YYYY-MM-DD H", tz=tz
    )

    return str(datetime)
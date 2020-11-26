#! /usr/bin/env python3

import sys, os, zipfile, json
import mysql.connector
from xml.dom import minidom
from datetime import datetime, timezone
from collections import namedtuple
from itertools import zip_longest
import pytz

Record = namedtuple("Record", "type datetime value")


# From https://docs.python.org/3/library/itertools.html#recipes
def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def get_max_datetime(creds):
    mydb = mysql.connector.connect(
        host=creds["host"],
        user=creds["user"],
        passwd=creds["pass"],
        database=creds["database"],
    )
    mycursor = mydb.cursor()

    sql = "SELECT max(datetime) as max_datetime from health_stats"
    mycursor.execute(sql)

    # There will be one or none - assumes MySQL is in UTC
    for (max_datetime,) in mycursor:
        if max_datetime:
            return max_datetime.replace(tzinfo=timezone.utc)
    return None


def read_records(fn, datetime_to_start=None):
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"

    TYPE_MAP = {
        "HKQuantityTypeIdentifierBodyMass": "BodyMass",
        "HKQuantityTypeIdentifierHeartRate": "HeartRate",
        "HKQuantityTypeIdentifierStepCount": "Steps",
    }

    TYPES = set(TYPE_MAP.keys())

    if datetime_to_start is None:
        datetime_to_start = datetime.strptime("1970-01-01", "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )

    if fn.endswith(".zip"):
        print("Handling as zip file")
        with zipfile.ZipFile(fn) as fz:
            with fz.open(os.path.join("apple_health_export", "export.xml")) as f:
                xmldoc = minidom.parseString(f.read())
    elif os.path.isdir(fn):
        print("Handling as directory")
        xmldoc = minidom.parse(os.path.join(fn, "export.xml"))
    else:
        print("Handling as export.xml file")
        xmldoc = minidom.parse(fn)

    for s in xmldoc.getElementsByTagName("Record"):
        if s.attributes["type"].value in TYPES:
            dt = datetime.strptime(s.attributes["startDate"].value, DATETIME_FORMAT)
            if dt > datetime_to_start:
                val = s.attributes["value"].value
                yield Record(
                    TYPE_MAP[s.attributes["type"].value], dt.astimezone(pytz.UTC), val
                )


def save_records(creds, record_generator):
    BATCH_SIZE = 100

    mydb = mysql.connector.connect(
        host=creds["host"],
        user=creds["user"],
        passwd=creds["pass"],
        database=creds["database"],
    )
    mycursor = mydb.cursor()

    sql = "INSERT INTO health_stats (type, datetime, value) VALUES (%s, %s, %s)"
    for batch in grouper(BATCH_SIZE, record_generator):
        vals = []
        for record in batch:
            if record is not None:
                vals.append((record.type, record.datetime, record.value))
        mycursor.executemany(sql, vals)
        mydb.commit()
        print(mycursor.rowcount, "record inserted.")


if __name__ == "__main__":
    with open("config.json", "r") as config_f:
        config = json.load(config_f)

    if len(sys.argv) > 1:
        export_file = sys.argv[1]
    else:
        export_file = "export.xml"

    save_records(config["db"], read_records(export_file, get_max_datetime(config["db"])))

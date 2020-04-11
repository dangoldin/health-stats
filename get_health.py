import json
import mysql.connector
from xml.dom import minidom
from datetime import datetime
from collections import defaultdict, namedtuple
from itertools import zip_longest

Record = namedtuple('Record', 'type datetime value')

# From https://docs.python.org/3/library/itertools.html#recipes
def grouper(n, iterable, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

def read_records(fn):
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S %z'

    TYPE_MAP = {
        'HKQuantityTypeIdentifierBodyMass': 'BodyMass',
        'HKQuantityTypeIdentifierHeartRate': 'HeartRate',
        'HKQuantityTypeIdentifierStepCount': 'Steps',
    }

    TYPES = set(TYPE_MAP.keys())

    xmldoc = minidom.parse(fn)
    for s in xmldoc.getElementsByTagName('Record'):
        if s.attributes['type'].value in TYPES:
            dt = datetime.strptime(s.attributes['startDate'].value, DATETIME_FORMAT)
            val = s.attributes['value'].value
            yield Record(TYPE_MAP[s.attributes['type'].value], dt, val)

def save_records(creds, record_generator):
    BATCH_SIZE = 50

    mydb = mysql.connector.connect(
        host = creds['host'],
        user = creds['user'],
        passwd = creds['pass'],
        database = creds['database'],
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

if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)

    save_records(config['db'], read_records('export.xml'))

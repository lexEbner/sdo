import numpy as np
import time

from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

url = "http://localhost:8086"
token = "RDHByh4KFzx5ESBzebaP9UzhSHd2FvQseZvypwTJWUNFG_hfshHclChF9o-dJSF0KEeexqwkAZcTwr5Kr6gAHA=="
org = "TUWien"
bucket = "manufacturing"


def make_prob_generator(lower, upper, weight, counter_max):
    value = np.random.randint(lower, upper)
    counter = 0
    def next_prob_value():
        nonlocal value, counter
        if counter != 0:
            if counter < counter_max:
                value += np.random.randint(1, 10) * weight
            else:
                value = np.random.randint(lower, upper)
                counter = 0
        counter += 1
        return round(value, 3)
    return next_prob_value

def make_lin_generator(init_value, increment, counter_max):
    value = init_value
    counter = 0
    def next_lin_value():
        nonlocal value, counter
        if counter != 0:
            if counter < counter_max:
                value += increment
            else:
                value = init_value
                counter = 0
        counter += 1
        return round(value, 3)
    return next_lin_value

def make_sin_generator(amplitude, omega, phase, offset):
    counter = 0
    def next_sin_value():
        nonlocal counter
        value = offset + amplitude * np.sin(omega * counter + phase)
        counter += 1
        return round(value, 3)
    return next_sin_value

def make_rand_int_generator(lower, upper):
    def rand_int():
        return np.random.randint(lower, upper)
    return rand_int

generators = {
    "Room1": {
        "Temperature": make_prob_generator(15, 22, 0.41, 5)
    },
    "Press": {
        "MaterialTemperature": make_rand_int_generator(50, 120),
        "Pressure": make_lin_generator(0, 15.7, 15)
    },
    "Extruder": {
        "Temperature": make_sin_generator(20, 2*np.pi / 5.1234, 1, 75),
        "Throughput": make_prob_generator(400, 405, 0.23, 11)
    },
    "Trimmer": {
        "Pressure": make_prob_generator(280, 300, 0.17, 13),
        "ZPosition": make_lin_generator(15, -1.5, 10)
    }
}

points = {
    "Room1": Point("machine_data").tag("Factory", "VIE").tag("Room", "Room1"),
    "Press": Point("machine_data").tag("Factory", "VIE").tag("Room", "Room1").tag("Line", "Line1").tag("Machine", "Press"),
    "Extruder": Point("machine_data").tag("Factory", "VIE").tag("Room", "Room1").tag("Line", "Line1").tag("Machine", "Extruder"),
    "Trimmer": Point("machine_data").tag("Factory", "VIE").tag("Room", "Room1").tag("Line", "Line2").tag("Machine", "Trimmer")
}

def fetch_record():
    record = []
    for machine, point in points.items():
        for field_key, field_value in generators[machine].items():
            # print(f"{field_key}={field_value()}")
            record.append(point.field(field_key, float(field_value())))
    return record

client = InfluxDBClient(url=url, token=token, org=org)

with client:
    delete_api = client.delete_api()
    start = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    stop = datetime.now(timezone.utc)

    delete_api.delete(start=start, stop=stop, bucket=bucket, predicate='_measurement="machine_data"')

    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    i = 0
    while i < 600:
        # fetch_record()
        write_api.write(bucket=bucket, record=fetch_record())
        time.sleep(1 + np.random.uniform(-0.1, 0.1))
        i += 1

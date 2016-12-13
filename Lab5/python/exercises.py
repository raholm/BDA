from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

if not "sc" in locals() or not "sc" in globals():
    sc = SparkContext()

def kernel(u):
    return exp(-u^2)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def kernel_model():
    h_distance = 100000 # Up to you
    h_date = 7 # Up to you
    h_time = 2 # Up to you

    a = 58.4274 # Up to you
    b = 14.826 # Up to you

    pred_date = datetime.date(datetime.strptime("2013-07-04", "%Y-%m-%d"))
    pred_time = datetime.time(datetime.strptime("08:00:00", "%H:%M:%S"))
    pred_datetime = datetime.combine(pred_date, pred_time)

    stations = sc.textFile("../data/stations.csv")
    temps = sc.textFile("../data/temps50k.csv")

    stations = stations.map(lambda line: line.split(",")) \
                       .map(lambda obs: (int(obs[0]),
                                         (float(obs[3]),
                                          float(obs[4]))))

    temps = temps.filter(lambda line: len(line) > 0) \
                 .map(lambda line: line.split(",")) \
                 .map(lambda obs: (int(obs[0]),
                                   (datetime.date(datetime.strptime(obs[1], "%Y-%m-%d")),
                                    datetime.time(datetime.strptime(obs[2], "%H:%M:%S")),
                                    float(obs[3]))))

    combined_data = stations.join(temps)
    combined_data.cache()

    combined_data = combined_data.filter(lambda obs: filter_date(x=obs, dt=pred_datetime))

    print(combined_data.count())


def filter_date(x, dt):
    # x - (station, ((lat, long), (date, time, temp)))
    merged_raw = datetime.combine(x[1][1][0], x[1][1][1])
    return merged_raw <= dt

def main():
    kernel_model()

main()


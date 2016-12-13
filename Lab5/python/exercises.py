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
    date = "2013-07-04" # Up to you

    date = datetime.date(datetime.strptime("2013-07-04", "%Y-%m-%d"))
    time = datetime.time(datetime.strptime("08:00:00", "%H:%M:%S"))

    stations = sc.textFile("../data/stations.csv")
    temps = sc.textFile("../data/temps50k.csv")

    stations = stations.map(lambda line: line.split(",")) \
                       .map(lambda obs: (int(obs[0]), (float(obs[3]), float(obs[4]))))

    stations.collect()

    temps = temps.filter(lambda line: len(line) > 0) \
                 .map(lambda line: line.split(",")) \
                 .map(lambda obs: (int(obs[0]), (datetime.date(datetime.strptime(obs[1], "%Y-%m-%d")),
                                                 datetime.time(datetime.strptime(obs[2], "%H:%M:%S")),
                                                 float(obs[3]))))

    temps.collect()
    # print(temps.take(5))
    # print(stations.take(5))

    combined = stations.join(temps)
    combined.cache()


    # data.filter(lambda obs: date_filter(obs, pred_date, pred_time))
    combined = combined.filter(lambda obs: filter_date(x=obs, date=date, time=time))


    print(date)
    print(time)
    print(datetime.combine(date, time))
    # print(combined.take(5))


def filter_date(x, date, time):
    merged_pred = datetime.combine(date, time)
    merged_raw = datetime.combine(x[1][1][0], x[1][1][1])
    # (station, ((long, lat), (date, time, temp)))
    return merged_raw <= merged_pred

def main():
    kernel_model()

main()


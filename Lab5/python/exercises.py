from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

if not "sc" in locals() or not "sc" in globals():
    sc = SparkContext()

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

    pred_latitude = 58.409158 # Up to you
    pred_longitude = 15.607452 # Up to you

    pred_date = datetime.strptime("2013-06-24", "%Y-%m-%d")
    pred_time = datetime.strptime("14:00:00", "%H:%M:%S")

    pred_times = [datetime.strptime(time, "%H:%M:%S")
                  for time in ["04:00:00", "06:00:00", "08:00:00", "10:00:00",
                               "12:00:00", "14:00:00", "16:00:00", "18:00:00",
                               "20:00:00", "22:00:00", "00:00:00"]]

    stations = sc.textFile("../data/stations.csv")
    temps = sc.textFile("../data/temps50k.csv")

    # (station, (latitude, longitude))
    stations = stations.map(lambda line: line.split( ",")) \
                       .map(lambda obs: (int(obs[0]),
                                         (float(obs[3]),
                                          float(obs[4]))))
    # (station, (date, time, temperature))
    temps = temps.filter(lambda line: len(line) > 0) \
                 .map(lambda line: line.split(",")) \
                 .map(lambda obs: (int(obs[0]),
                                   (datetime.strptime(obs[1], "%Y-%m-%d"),
                                    datetime.strptime(obs[2], "%H:%M:%S"),
                                    float(obs[3]))))

    stations_data = stations.collectAsMap()
    stations_data = sc.broadcast(stations_data).value

    # (station, (longitude, latitude, date, time, temperature))
    combined_data = temps.map(lambda (station, (date, time, temp)):
                              (station, (stations_data[station][1],
                                         stations_data[station][0],
                                         date, time, temp)))
    combined_data.cache()

    result = [combined_data.filter(lambda x:
                                   filter_date(x=x,
                                               dt=datetime.combine(datetime.date(pred_date),
                                                                   datetime.time(pred_time)))) \
              .map(lambda x: (None,
                              (kernel(x, pred_longitude,
                                      pred_latitude, h_distance,
                                      pred_date, h_date,
                                      pred_time, h_time) *  get_temp(x),
                               kernel(x, pred_longitude,
                                      pred_latitude, h_distance,
                                      pred_date, h_date,
                                      pred_time, h_time)))) \
              .reduceByKey(lambda (estimate1, kernel1), (estimate2, kernel2):
                           (estimate1 + estimate2, kernel1 + kernel2)) \
              .map(lambda (key, (estimate, kernel)): (key, estimate / kernel)) \
              .collect()[0][1]
              for pred_time in pred_times]

    print(result)

def filter_date(x, dt):
    merged_raw = datetime.combine(datetime.date(get_date(x)),
                                  datetime.time(get_time(x)))
    return merged_raw <= dt

def gaussian_kernel(u):
    return exp(-u**2)

def date_kernel(x, date, h):
    distance = (get_date(x) - date).days
    return gaussian_kernel(distance / h)

def time_kernel(x, time, h):
    seconds_per_hour = 3600
    distance = (get_time(x) - time).seconds / seconds_per_hour

    if distance > 12:
        distance = 24 - distance

    return gaussian_kernel(distance / h)

def distance_kernel(x, longitude, latitude, h):
    distance = haversine(get_longitude(x), get_latitude(x),
                         longitude, latitude)
    return gaussian_kernel(distance / h)

def kernel(x, longitude, latitude, h_dist, date, h_date, time, h_time):
    kern = (distance_kernel(x, longitude, latitude, h_dist) +
            date_kernel(x, date, h_date) + time_kernel(x, time, h_time))
    return kern

def get_temp(x):
    return x[1][-1]

def get_longitude(x):
    return x[1][0]

def get_latitude(x):
    return x[1][1]

def get_date(x):
    return x[1][2]

def get_time(x):
    return x[1][3]

def main():
    kernel_model()

main()


def exercise01():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda observation:
                               (int(observation[1][0:4]) >= 1950 and
                                int(observation[1][0:4]) <= 2014)) \
                       .cache()

    exercise01question(observations)
    exercise01a(observations)

def exercise01question(observations):
    temperatures = observations.map(lambda observation:
                                    (observation[1][0:4], float(observation[3])))
    max_temperatures = temperatures.reduceByKey(max) \
                                   .sortBy(ascending=False,
                                           keyfunc=lambda (year, temp): temp)

    min_temperatures = temperatures.reduceByKey(min) \
                                   .sortBy(ascending=True,
                                           keyfunc=lambda (year, temp): temp)

    print("Max:", max_temperatures.take(5))
    print("Min:", min_temperatures.take(5))

def exercise01a(observations):
    station_temperatures = observations.map(lambda observation:
                                            (observation[1][0:4],
                                             (observation[0], float(observation[3]))))

    max_temperatures_station = station_temperatures.reduceByKey(lambda (station1, temp1), (station2, temp2):
                                                                (station1, temp1)
                                                                if temp1 > temp2 else
                                                                (station2, temp2)) \
                                                   .sortBy(ascending=False,
                                                           keyfunc=lambda (year, (station, temp)): temp)

    min_temperatures_station = station_temperatures.reduceByKey(lambda (station1, temp1), (station2, temp2):
                                                                (station1, temp1)
                                                                if temp1 < temp2 else
                                                                (station2, temp2)) \
                                                   .sortBy(ascending=True,
                                                           keyfunc=lambda (year, (station, temp)): temp)

    print("Max (station):", max_temperatures_station.take(5))
    print("Min (station):", min_temperatures_station.take(5))

def exercise02():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda observation:
                               (int(observation[1][:4]) >= 1950 and
                                int(observation[1][:4]) <= 2014)) \
                       .cache()

    exercise02a(observations)
    exercise02b(observations)

def exercise02a(observations):
    temperatures = observations.map(lambda observation:
                                    (observation[1][:7], (float(observation[3]), 1))) \
                               .filter(lambda (month, (temp, count)): temp > 10)
    reading_counts = temperatures.reduceByKey(lambda (temp1, count1), (temp2, count2):
                                              (temp1, count1 + count2)) \
                                 .map(lambda (month, (temp, count)):
                                      (month, count))

    print(reading_counts.take(5))

def exercise02b(observations):
    station_temperatures = observations.map(lambda observation:
                                            (observation[1][:7],
                                             (observation[0], float(observation[3])))) \
                                       .filter(lambda (month, (station, temp)): temp > 10)

    year_station = station_temperatures.map(lambda (month, (station, temp)): (month, (station, 1))).distinct()
    reading_counts = year_station.reduceByKey(lambda (station1, count1), (station2, count2):
                                              (station1, count1 + count2)) \
                                 .map(lambda (month, (station, count)): (month, count))

    print(reading_counts.take(5))


def exercise03():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";"))
    observations = observations.filter(lambda observation:
                                       (int(observation[1][:4]) >= 1960 and
                                        int(observation[1][:4]) <= 2014))

    station_day_temperatures = observations.map(lambda observation:
                                                ((observation[1], observation[0]),
                                                 (float(observation[3]), float(observation[3]))))

    station_day_minmax_temps = station_day_temperatures.reduceByKey(lambda
                                                                    (mintemp1, maxtemp1),
                                                                    (mintemp2, maxtemp2):
                                                                    (min(mintemp1, mintemp2),
                                                                     max(maxtemp1, maxtemp2)))

    station_month_avg_temps = station_day_minmax_temps.map(lambda ((day, station), (mintemp, maxtemp)):
                                                           ((day[:7], station), (sum((mintemp, maxtemp)), 2))) \
                                                      .reduceByKey(lambda (temp1, count1), (temp2, count2):
                                                                   (temp1 + temp2, count1 + count2)) \
                                                      .map(lambda ((month, station), (temp, count)):
                                                           ((month, station), temp / float(count)))

    print(station_month_avg_temps.take(5))

def exercise04():
    temperature_data = sc.textFile("../data/temperature-readings.csv")
    precipitation_data = sc.textFile("../data/precipitation-readings.csv")

    temp_obs = temperature_data.map(lambda line: line.split(";")) \
                               .map(lambda obs: ((obs[1], int(obs[0])), float(obs[3]))) \
                               .reduceByKey(max) \
                               .filter(lambda ((day, station), temp):
                                       temp >= 25 and temp <= 30 ) \
                               .map(lambda ((day, station), temp):
                                    (station, day)) \
                               .groupByKey()

    precip_obs = precipitation_data.map(lambda line: line.split(";")) \
                                   .map(lambda obs: ((obs[1], int(obs[0])), float(obs[3]))) \
                                   .reduceByKey(lambda precip1, precip2: precip1 + precip2) \
                                   .filter(lambda ((day, station), precip):
                                           precip >= 100 and precip <= 200) \
                                   .map(lambda ((day, station), precip):
                                        (station, day)) \
                                   .groupByKey()

    combined = temp_obs.join(precip_obs)

    print(combined.take(5))

def exercise05():
    station_data = sc.textFile("../data/stations-Ostergotland.csv")

    stations = station_data.map(lambda line: line.split(";")) \
                           .map(lambda obs: int(obs[0])) \
                           .distinct().collect()
    stations = {station: True for station in stations}

    precipitation_data = sc.textFile("../data/precipitation-readings.csv")

    precipitation_daily = precipitation_data.map(lambda line: line.split(";")) \
                                            .filter(lambda obs: stations.get(int(obs[0]), False)) \
                                            .map(lambda obs: ((obs[1], obs[2]), float(obs[3]))) \
                                            .map(lambda ((day, time), precip): (day, precip)) \
                                            .reduceByKey(lambda precip1, precip2:
                                                         precip1 + precip2)

    precipitation_avg_month =  precipitation_daily.map(lambda (day, precip):
                                                       (day[:7], (precip, 1))) \
                                                  .reduceByKey(lambda (precip1, count1),
                                                               (precip2, count2):
                                                               (precip1 + precip2,
                                                                count1 + count2)) \
                                                  .map(lambda (month, (precip, count)):
                                                       (month, precip / float(count)))

    print(precipitation_avg_month.take(5))

def exercise06():
    station_data = sc.textFile("../data/stations-Ostergotland.csv")

    stations = station_data.map(lambda line: line.split(";")) \
                           .map(lambda obs: int(obs[0])) \
                           .distinct().collect()
    stations = {station: True for station in stations}

    """
    How to extract the observations from the stations in ostergotland.

    grep -E '75520|85250|85130|85390|85650|86420|85270|85280|85410|84260|86440|86130|85040|86200|86330|85180|86090|86340|86470|85450|86350|85460|86360|85220|85210|85050|85600|86370|87140|87150|85160|85490|85240|85630' temperature-readings.csv > temperature-readings-ostergotland.csv
    """

    temperature_data = sc.textFile("../data/temperature-readings-ostergotland.csv")

    temperature_data_filtered = temperature_data.map(lambda line: line.split(";")) \
                                                .filter(lambda obs:
                                                        stations.get(int(obs[0]), False)) \
                                                .filter(lambda obs:
                                                        (int(obs[1][:4]) >= 1950 and
                                                         int(obs[1][:4]) <= 2014))

    month_avg_temp = temperature_data_filtered.map(lambda obs:
                                                   ((obs[1], int(obs[0])),
                                                    (float(obs[3]), float(obs[3])))) \
                                              .reduceByKey(lambda (mint1, maxt1), (mint2, maxt2):
                                                           (min(mint1, mint2), max(maxt1, maxt2))) \
                                              .map(lambda ((day, station), (mint, maxt)):
                                                   (day[:7], (mint + maxt, 2))) \
                                              .reduceByKey(lambda (temp1, count1), (temp2, count2):
                                                           (temp1 + temp2, count1 + count2)) \
                                              .map(lambda (month, (temp, count)):
                                                   (month, temp / float(count))) \
                                              .sortBy(ascending=True, keyfunc=lambda (month, temp):
                                                      month)

    month_longterm_avg_temp = month_avg_temp.filter(lambda (month, temp):
                                                    int(month[:4]) <= 1980) \
                                            .map(lambda (month, temp):
                                                 (month[-2:], (temp, 1))) \
                                            .reduceByKey(lambda (temp1, count1), (temp2, count2):
                                                         (temp1 + temp2, count1 + count2)) \
                                            .map(lambda (month, (temp, count)):
                                                 (month, temp / float(count))) \
                                            .sortBy(ascending=True, keyfunc=lambda (month, temp):
                                                    month)

    print(month_avg_temp.take(5))
    print(month_longterm_avg_temp.take(5))
    # month_avg_temp.repartition(1).saveAsTextFile("../result/6_1")
    # month_longterm_avg_temp.repartition(1).saveAsTextFile("../result/6_2")

def main():
    # exercise01()
    # exercise02()
    # exercise03()
    exercise04()
    # exercise05()
    # exercise06()


main()

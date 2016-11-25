def exercise01():
    data = sc.textFile("../data/temperature-readings.csv").cache()

    observations = data.map(lambda line: line.split(";"))
    observations = observations.filter(lambda observation: (int(observation[1][0:4]) >= 1950 and
                                                            int(observation[1][0:4]) <= 2014))
    temperatures = observations.map(lambda observation: (observation[1][0:4], observation[3]))
    max_temperatures = temperatures.reduceByKey(max)
    max_temperatures = max_temperatures.sortBy(ascending=False, keyfunc=lambda (key, value): value)

    min_temperatures = temperatures.reduceByKey(min)
    min_temperatures = min_temperatures.sortBy(ascending=False, keyfunc=lambda (key, value): value)


    station_temperatures = data.map(lambda row: (row[1][0:4], (row[0], float(row[3]))))

    max_temperatures_station = station_temperatures.reduceByKey(lambda v1, v2: v1 if v1[1] > v2[1] else v2)
    max_temperatures_station = max_temperatures_station.sortBy(ascending=False,
                                                               keyfunc=lambda (key, value): value[1])

    print(max_temperatures_station.take(5))

def exercise02():
    pass

def exercise03():
    pass

def exercise04():
    pass

def exercise05():
    pass

def exercise06():
    pass

def main():
    exercise01()
    exercise02()
    exercise03()
    exercise04()
    exercise05()
    exercise06()

if __name__ == "__main__":
    main()

main()

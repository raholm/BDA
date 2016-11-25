def exercise01():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";"))
    observations = observations.filter(lambda observation: (int(observation[1][0:4]) >= 1950 and
                                                            int(observation[1][0:4]) <= 2014))

    # 1
    temperatures = observations.map(lambda observation: (observation[1][0:4], float(observation[3])))
    max_temperatures = temperatures.reduceByKey(max)
    max_temperatures = max_temperatures.sortBy(ascending=False, keyfunc=lambda (year, temp): temp)

    min_temperatures = temperatures.reduceByKey(min)
    min_temperatures = min_temperatures.sortBy(ascending=True, keyfunc=lambda (year, temp): temp)

    print("Max:", max_temperatures.take(5))
    print("Min:", min_temperatures.take(5))

    # 1a)
    station_temperatures = observations.map(lambda observation: (observation[1][0:4], (observation[0],
                                                                                       float(observation[3]))))

    max_temperatures_station = station_temperatures.reduceByKey(lambda (station1, temp1), (station2, temp2):
                                                                (station1, temp1)
                                                                if temp1 > temp2 else
                                                                (station2, temp2))
    max_temperatures_station = max_temperatures_station.sortBy(ascending=False,
                                                               keyfunc=lambda (year, (station, temp)): temp)

    min_temperatures_station = station_temperatures.reduceByKey(lambda (station1, temp1), (station2, temp2):
                                                                (station1, temp1)
                                                                if temp1 < temp2 else
                                                                (station2, temp2))
    min_temperatures_station = min_temperatures_station.sortBy(ascending=True,
                                                               keyfunc=lambda (year, (station, temp)): temp)

    print("Max (station):", max_temperatures_station.take(5))
    print("Min (station):", min_temperatures_station.take(5))

    # 1b)

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

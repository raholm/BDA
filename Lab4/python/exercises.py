"""
Exercise 1:
Year, station with the max, max
Year, station with the min, min

Exercise 2:
Year-month, number
Year-month, distinct number

Exercise 3:
Year-month-station, average temperature

Exercise 4:
Station, max Temperature, max daily precipitation

Exercise 5:
Year-month, avg monthly precipitation

Exercise 6:
Year-month, difference
"""



def exercise01():
    data = sc.textFile("../data/temperature-readings.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda observation:
                               (int(observation[1][:4]) >= 1950 and
                                int(observation[1][:4]) <= 2014)) \
                       .cache()

def exercise02():
    pass

def exercise03():
    pass

def exercise04():
    pass

def exercise06():
    pass

def main():
    exercise01()
    # exercise02()
    # exercise03()
    # exercise04()
    # exercise05()
    # exercise06()


main()

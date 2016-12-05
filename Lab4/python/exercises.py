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
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, functions

if not "sc" in locals() or not "sc" in globals():
    sc = SparkContext()

if not "sqlContext" in locals() or not "sqlContext" in globals():
    sqlContext = SQLContext(sc)

def exercise01():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda obs:
                               (int(obs[1][:4]) >= 1950 and
                                int(obs[1][:4]) <= 2014)) \
                       .map(lambda obs: Row(station=obs[0],
                                            year=obs[1].split("-")[0],
                                            temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    year_min_temp = sqlContext.sql(
        """
        SELECT DISTINCT(year) AS year, station, temp
        FROM
        (
        SELECT year, station, temp, MIN(temp) OVER (PARTITION BY year) min_temp
        FROM temp_readings
        )
        WHERE temp = min_temp
        """
    )

    year_max_temp = sqlContext.sql(
        """
        SELECT DISTINCT(year) AS year, station, temp
        FROM
        (
        SELECT year, station, temp, MAX(temp) OVER (PARTITION BY year) max_temp
        FROM temp_readings
        )
        WHERE temp = max_temp
        """
    )

    print(year_min_temp.take(10))
    print(year_max_temp.take(10))

def exercise02():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda obs:
                               (int(obs[1][:4]) >= 1950 and
                                int(obs[1][:4]) <= 2014)) \
                       .map(lambda obs: Row(station=obs[0],
                                            month=obs[1][:7],
                                            temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    month_count = sqlContext.sql(
        """
        SELECT month, COUNT(*) AS count
        FROM temp_readings
        WHERE temp > 10
        GROUP BY month
        """
    )

    month_distinct_count = sqlContext.sql(
        """
        SELECT month, COUNT(DISTINCT(station)) AS count
        FROM temp_readings
        WHERE temp > 10
        GROUP BY month
        """
    )

    print(month_count.take(5))
    print(month_distinct_count.take(5))

def exercise03():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda obs:
                               (int(obs[1][:4]) >= 1950 and
                                int(obs[1][:4]) <= 2014)) \
                       .map(lambda obs: Row(station=obs[0],
                                            day=obs[1],
                                            temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    station_day_minmax_temps = sqlContext.sql(
        """
        SELECT day, station, MIN(temp) AS min_temp, MAX(temp) AS max_temp
        FROM temp_readings
        GROUP BY day, station
        """
    )

    print(station_day_minmax_temps.take(5))


def exercise04():
    pass

def exercise05():
    pass

def exercise06():
    pass

def main():
    # exercise01()
    # exercise02()
    exercise03()
    # exercise04()
    # exercise05()
    # exercise06()


main()

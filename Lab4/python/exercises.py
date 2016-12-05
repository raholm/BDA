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

    exercise01question()
    exercise01a()

def exercise01question():
    year_min_temp = sqlContext.sql(
        """
        SELECT year, MIN(temp) AS temp
        FROM temp_readings
        GROUP BY year
        ORDER BY temp ASC
        """
    )

    year_max_temp = sqlContext.sql(
        """
        SELECT year, MAX(temp) AS temp
        FROM
        temp_readings
        GROUP BY year
        ORDER BY temp DESC
        """
    )

    print(year_min_temp.take(5))
    print(year_max_temp.take(5))

def exercise01a():
    year_min_temp = sqlContext.sql(
        """
        SELECT DISTINCT(year) AS year, station, temp
        FROM
        (
        SELECT year, station, temp, MIN(temp) OVER (PARTITION BY year) min_temp
        FROM temp_readings
        )
        WHERE temp = min_temp
        ORDER BY temp ASC
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
        ORDEr BY temp DESC
        """
    )

    print(year_min_temp.take(5))
    print(year_max_temp.take(5))

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

    exercise02a()
    exercise02b()

def exercise02a():
    month_count = sqlContext.sql(
        """
        SELECT month, COUNT(*) AS count
        FROM temp_readings
        WHERE temp > 10
        GROUP BY month
        """
    )

    print(month_count.take(5))

def exercise02b():

    month_distinct_count = sqlContext.sql(
        """
        SELECT month, COUNT(DISTINCT(station)) AS count
        FROM temp_readings
        WHERE temp > 10
        GROUP BY month
        """
    )

    print(month_distinct_count.take(5))

def exercise03():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda obs:
                               (int(obs[1][:4]) >= 1960 and
                                int(obs[1][:4]) <= 2014)) \
                       .map(lambda obs: Row(station=obs[0],
                                            day=obs[1],
                                            month=obs[1][:7],
                                            temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    station_day_minmax_temps = sqlContext.sql(
        """
        SELECT month, station, AVG(max_temp + min_temp) / 2 AS avg_temp
        FROM
        (
        SELECT month, station, MIN(temp) AS min_temp, MAX(temp) AS max_temp
        FROM temp_readings
        GROUP BY day, month, station
        )
        GROUP BY month, station
        ORDER BY month
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

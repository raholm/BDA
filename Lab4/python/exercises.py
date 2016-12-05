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
                                            date=obs[1],
                                            year=obs[1].split("-")[0],
                                            month=obs[1].split("-")[1],
                                            day=obs[1].split("-")[2],
                                            time=obs[2],
                                            value=float(obs[3]),
                                            quality=obs[4]))

    # schema_temp_readings_names = ["station", "date", "year", "month", "dat", "time", "value", "quality"]
    # schema_temp_readings = sqlContext.createDataFrame(observations, schema_temp_readings_names)
    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    year_min_temp = sqlContext.sql(
        """
        SELECT DISTINCT(year) AS year, station, value
        FROM
        (
        SELECT year, station, value, MIN(value) OVER (PARTITION BY year) max_value
        FROM temp_readings
        )
        WHERE value = max_value
        """
    )

    year_max_temp = sqlContext.sql(
        """
        SELECT DISTINCT(year) AS year, station, value
        FROM
        (
        SELECT year, station, value, MAX(value) OVER (PARTITION BY year) max_value
        FROM temp_readings
        )
        WHERE value = max_value
        """
    )


    print(year_min_temp.take(10))
    print(year_max_temp.take(10))

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

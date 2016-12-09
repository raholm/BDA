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
                       .map(lambda obs: Row(station=int(obs[0]),
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
        ORDER BY temp DESC
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

    # print(year_min_temp.take(5))
    # print(year_max_temp.take(5))

    year_min_temp.rdd.repartition(1).saveAsTextFile("../nsc_result/1_qa")
    year_max_temp.rdd.repartition(1).saveAsTextFile("../nsc_result/1_qb")


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

    year_min_temp.rdd.repartition(1).saveAsTextFile("../nsc_result/1_aa")
    year_max_temp.rdd.repartition(1).saveAsTextFile("../nsc_result/1_ab")

def exercise02():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda obs:
                               (int(obs[1][:4]) >= 1950 and
                                int(obs[1][:4]) <= 2014)) \
                       .map(lambda obs: Row(station=int(obs[0]),
                                            month=obs[1][:7],
                                            temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    # exercise02a()
    exercise02aAPI(schema_temp_readings)
    # exercise02b()
    exercise02bAPI(schema_temp_readings)

def exercise02aAPI(table):
    month_count = table.filter(table["temp"] > 10) \
                       .groupBy("month") \
                       .agg(functions.count("*").alias("count")) \
                       .orderBy(functions.count("*").desc())

    month_counts.rdd.repartition(1).saveAsTextFile("../nsc_result/2_a")

def exercise02a():
    month_count = sqlContext.sql(
        """
        SELECT month, COUNT(*) AS count
        FROM temp_readings
        WHERE temp > 10
        GROUP BY month
        ORDER BY COUNT(*) DESC
        """
    )

    print(month_count.take(5))

def exercise02bAPI(table):
    month_distinct_count = table.filter(table["temp"] > 10) \
                                .groupBy("month") \
                                .agg(functions.countDistinct("station").alias("count")) \
                                .orderBy(functions.countDistinct("station").desc())

    # print(month_distinct_count.take(5))
    month_distinct_count.rdd.repartition(1).saveAsTextFile("../nsc_result/2_b")

def exercise02b():
    month_distinct_count = sqlContext.sql(
        """
        SELECT month, COUNT(DISTINCT(station)) AS count
        FROM temp_readings
        WHERE temp > 10
        GROUP BY month
        ORDER BY COUNT(DISTINCT(station)) DESC
        """
    )

    print(month_distinct_count.take(5))

def exercise03():
    data = sc.textFile("../data/temperature-readings-small.csv")

    observations = data.map(lambda line: line.split(";")) \
                       .filter(lambda obs:
                               (int(obs[1][:4]) >= 1960 and
                                int(obs[1][:4]) <= 2014)) \
                       .map(lambda obs: Row(station=int(obs[0]),
                                            day=obs[1],
                                            month=obs[1][:7],
                                            temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(observations)
    schema_temp_readings.registerTempTable("temp_readings")

    station_month_avg_temps = sqlContext.sql(
        """
        SELECT month, station, AVG(max_temp + min_temp) / 2 AS avg_temp
        FROM
        (
        SELECT month, station, MIN(temp) AS min_temp, MAX(temp) AS max_temp
        FROM temp_readings
        GROUP BY day, month, station
        )
        GROUP BY month, station
        ORDER BY AVG(max_temp + min_temp) / 2 DESC
        """
    )

    # print(station_month_avg_temps.take(5))
    station_month_avg_temps.rdd.repartition(1).saveAsTextFile("../nsc_result/3")


def exercise04():
    temperature_data = sc.textFile("../data/temperature-readings-small2.csv")
    precipitation_data = sc.textFile("../data/precipitation-readings.csv")

    temperature_obs = temperature_data.map(lambda line: line.split(";")) \
                                      .map(lambda obs: Row(station=int(obs[0]),
                                                           temp=float(obs[3])))

    precipitation_obs = precipitation_data.map(lambda line: line.split(";")) \
                                          .map(lambda obs: Row(station=int(obs[0]),
                                                               day=obs[1],
                                                               precip=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(temperature_obs)
    schema_temp_readings.registerTempTable("temp_readings")

    schema_precip_readings = sqlContext.createDataFrame(precipitation_obs)
    schema_precip_readings.registerTempTable("precip_readings")

    combined = sqlContext.sql(
        """
        SELECT tr.station, MAX(temp) AS max_temp, MAX(precip) AS max_precip
        FROM
        temp_readings AS tr
        INNER JOIN
        (
        SELECT station, SUM(precip) AS precip
        FROM precip_readings
        GROUP BY day, station
        ) AS pr
        ON tr.station = pr.station
        GROUP BY tr.station
        ORDER BY tr.station DESC
        """
    )

    # print(combined.take(5))
    combined.rdd.repartition(1).saveAsTextFile("../nsc_result/4")

def exercise05():
    station_data = sc.textFile("../data/stations-Ostergotland.csv")

    stations = station_data.map(lambda line: line.split(";")) \
                           .map(lambda obs: int(obs[0])) \
                           .distinct().collect()
    stations = sc.broadcast(stations)
    stations = {station: True for station in stations.value}

    precipitation_data = sc.textFile("../data/precipitation-readings.csv")

    precipitation_obs = precipitation_data.map(lambda line: line.split(";")) \
                                          .filter(lambda obs: stations.get(int(obs[0]), False)) \
                                          .map(lambda obs: Row(day=obs[1],
                                                               month=obs[1][:7],
                                                               station=int(obs[0]),
                                                               precip=float(obs[3])))

    schema_precip_readings = sqlContext.createDataFrame(precipitation_obs)
    schema_precip_readings.registerTempTable("precip_readings")

    precipitation_avg_month = sqlContext.sql(
        """
        SELECT month, AVG(precip) AS avg_precip
        FROM
        (
        SELECT month, station, SUM(precip) AS precip
        FROM
        (
        SELECT month, station, SUM(precip) AS precip
        FROM precip_readings
        GROUP BY day, month, station
        )
        GROUP BY month, station
        )
        GROUP BY month
        ORDER BY month DESC
        """
    )

    print(precipitation_avg_month.rdd.take(5))
    # precipitation_avg_month.rdd.repartition(1).saveAsTextFile("../nsc_result/5")

def exercise06():
    station_data = sc.textFile("../data/stations-Ostergotland.csv")

    stations = station_data.map(lambda line: line.split(";")) \
                           .map(lambda obs: int(obs[0])) \
                           .distinct()
    stations = sc.broadcast(stations)
    stations = {station: True for station in stations.value}

    temperature_data = sc.textFile("../data/temperature-readings-ostergotland.csv")

    temperature_data_filtered = temperature_data.map(lambda line: line.split(";")) \
                                                .filter(lambda obs:
                                                        (stations.get(int(obs[0]), False) and
                                                         int(obs[1][:4]) >= 1950 and
                                                         int(obs[1][:4]) <= 2014)) \
                                                .map(lambda obs: Row(station=int(obs[0]),
                                                                     day=obs[1],
                                                                     month=obs[1][:7],
                                                                     temp=float(obs[3])))

    schema_temp_readings = sqlContext.createDataFrame(temperature_data_filtered)
    schema_temp_readings.registerTempTable("temp_readings")

    month_avg_temp = sqlContext.sql(
        """
        SELECT month, AVG(max_temp + min_temp) / 2 AS avg_temp
        FROM
        (
        SELECT month, station, MIN(temp) AS min_temp, MAX(temp) AS max_temp
        FROM temp_readings
        GROUP BY day, month, station
        )
        GROUP BY month
        """
    )

    longterm_avg_temp = month_avg_temp.filter(functions.substring(month_avg_temp["month"], 1, 4) <= 1980) \
                                      .groupBy(functions.substring(month_avg_temp["month"], 6, 7).alias("month")) \
                                      .agg(functions.avg(month_avg_temp["avg_temp"]).alias("longterm_avg_temp"))

    result = month_avg_temp.join(longterm_avg_temp,
                                   (functions.substring(month_avg_temp["month"], 6, 7) ==
                                    longterm_avg_temp["month"]), "inner") \
                             .select(month_avg_temp["month"],
                                     (functions.abs(month_avg_temp["avg_temp"]) -
                                      functions.abs(longterm_avg_temp["longterm_avg_temp"])).alias("temp")) \
                             .orderBy(month_avg_temp["month"].desc())

    print(result.take(5))

    # result = sqlContext.sql(
    #     """
    #     SELECT avg.month, ABS(avg.avg_temp) - ABS(longterm.longterm_avg_temp) AS temp
    #     FROM
    #     (
    #     SELECT month, AVG(max_temp + min_temp) / 2 AS avg_temp
    #     FROM
    #     (
    #     SELECT month, station, MIN(temp) AS min_temp, MAX(temp) AS max_temp
    #     FROM temp_readings
    #     GROUP BY day, month, station
    #     )
    #     GROUP BY month
    #     ) AS avg
    #     INNER JOIN
    #     (
    #     SELECT SUBSTRING(month, 6, 7) AS month, AVG(avg_temp) AS longterm_avg_temp
    #     FROM
    #     (
    #     SELECT month, AVG(max_temp + min_temp) / 2 AS avg_temp
    #     FROM
    #     (
    #     SELECT month, station, MIN(temp) AS min_temp, MAX(temp) AS max_temp
    #     FROM temp_readings
    #     GROUP BY day, month, station
    #     )
    #     GROUP BY month
    #     )
    #     WHERE INT(SUBSTRING(month, 1, 4)) <= 1980
    #     GROUP BY SUBSTRING(month, 6, 7)
    #     ) AS longterm
    #     ON SUBSTRING(avg.month, 6, 7) = longterm.month
    #     ORDER BY month DESC
    #     """
    # )

    # print(result.take(5))

    result.repartition(1).saveAsTextFile("../nsc_result/6")

def main():
    # exercise01()
    # exercise02()
    # exercise03()
    # exercise04()
    exercise05()
    # exercise06()

main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, avg
from pyspark.sql.functions import year, month, dayofweek, col, when, hour, minute, datediff, expr

spark = SparkSession.builder.appName("FlightDelaysProcessing").getOrCreate()

# Data Import
df = spark.read.csv("s3a://flight-delays-data-bucket/flight_delays.csv", header=True, inferSchema=True)

print("First 5 rows of the dataset:")
df.show(5)

print("Schema of the dataset:")
df.printSchema()

print(f"Total number of rows in the dataset: {df.count()}")

# EDA / Data Preprocessing
negative_delay_with_reason = df.filter((col("DelayMinutes") < 0) & col("DelayReason").isNotNull())

count_negative_delay_with_reason = negative_delay_with_reason.count()

print(f"Number of rows where DelayMinutes is negative and DelayReason is NOT null: {count_negative_delay_with_reason}")

total_negative_delay = df.filter(col("DelayMinutes") < 0).count()

print(f"Total number of rows with negative DelayMinutes: {total_negative_delay}")

if count_negative_delay_with_reason == 0:
    print("All rows with negative DelayMinutes have a null DelayReason.")
else:
    print("Some rows with negative DelayMinutes have a non-null DelayReason.")

df = df.withColumn("DelayReason",when(col("DelayMinutes") <= 0, "On Time").otherwise(col("DelayReason")))

missing_values = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])

missing_values.show()

# Data Transformations
df = df.withColumn("IsDelayed", when(col("DelayMinutes") > 0, True).otherwise(False))

df = df.withColumn(
    "TimeOfDay",
    when((hour(col("ScheduledDeparture")) >= 5) & (hour(col("ScheduledDeparture")) < 12), "Morning") \
    .when((hour(col("ScheduledDeparture")) >= 12) & (hour(col("ScheduledDeparture")) < 17), "Afternoon") \
    .when((hour(col("ScheduledDeparture")) >= 17) & (hour(col("ScheduledDeparture")) < 21), "Evening") \
    .otherwise("Night")
)

df = df.withColumn(
    "DelayCategory",
    when(col("DelayMinutes") <= 0, "On Time") \
    .when(col("DelayMinutes") <= 15, "Short Delay") \
    .when(col("DelayMinutes") <= 60, "Moderate Delay") \
    .otherwise("Long Delay")
)


# Data Aggregation

total_delays_by_airline = df.filter(col("IsDelayed") == True) \
                            .groupBy("Airline") \
                            .agg(count("*").alias("TotalDelays")) \
                            .orderBy(col("TotalDelays").desc())

total_delays_by_airline.show()


avg_delay_by_airline = df.filter(col("IsDelayed") == True) \
                         .groupBy("Airline") \
                         .agg(avg("DelayMinutes").alias("AvgDelayMinutes")) \
                         .orderBy(col("AvgDelayMinutes").desc())

avg_delay_by_airline.show()

flights_by_time_of_day = df.groupBy("TimeOfDay") \
                           .agg(count("*").alias("TotalFlights")) \
                           .orderBy(col("TotalFlights").desc())

flights_by_time_of_day.show()

cancellations_and_diversions = df.groupBy("Airline") \
                                  .agg(
                                      count(when(col("Cancelled") == True, 1)).alias("TotalCancellations"),
                                      count(when(col("Diverted") == True, 1)).alias("TotalDiversions")
                                  ) \
                                  .orderBy(col("TotalCancellations").desc())

cancellations_and_diversions.show()

avg_delay_by_category = df.groupBy("DelayCategory") \
                          .agg(avg("DelayMinutes").alias("AvgDelayMinutes")) \
                          .orderBy(col("AvgDelayMinutes").desc())

avg_delay_by_category.show()

# Saving Data back to S3 bucket

output_s3_path = "s3a://flight-delays-data-bucket/flight_delays_transformed_single.csv"
output_path = "/home/ec2-user//fligh_delays_transformed_single.csv"

df.coalesce(1).write.csv(output_s3_path, header=True, mode="overwrite")


output_s3_path = "s3a://flight-delays-data-bucket/avg_delay_by_category"


# Spark SQL

# 1. Total Number of Delays by Airline
spark.sql("""
    SELECT Airline, COUNT(*) AS TotalDelays
    FROM flights
    WHERE IsDelayed = True
    GROUP BY Airline
    ORDER BY TotalDelays DESC
""").show()

# 2. Average Delay Time by Time of Day
spark.sql("""
    SELECT TimeOfDay, AVG(DelayMinutes) AS AvgDelay
    FROM flights
    WHERE IsDelayed = True
    GROUP BY TimeOfDay
    ORDER BY AvgDelay DESC
""").show()

# 3. Top 5 Longest Delays
spark.sql("""
    SELECT FlightID, Airline, Origin, Destination, DelayMinutes
    FROM flights
    ORDER BY DelayMinutes DESC
    LIMIT 5
""").show()

# 4. Number of Flights by Delay Category
spark.sql("""
    SELECT DelayCategory, COUNT(*) AS TotalFlights
    FROM flights
    GROUP BY DelayCategory
    ORDER BY TotalFlights DESC
""").show()

# 5. Top 5 Airports with the Most Departures
spark.sql("""
    SELECT Origin, COUNT(*) AS TotalDepartures
    FROM flights
    GROUP BY Origin
    ORDER BY TotalDepartures DESC
    LIMIT 5
""").show()

# 6. Percentage of Delayed Flights by Airline
spark.sql("""
    SELECT 
        Airline,
        COUNT(CASE WHEN IsDelayed = True THEN 1 END) * 100.0 / COUNT(*) AS DelayedPercentage
    FROM flights
    GROUP BY Airline
    ORDER BY DelayedPercentage DESC
""").show()
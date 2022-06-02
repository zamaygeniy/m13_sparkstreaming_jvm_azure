// Databricks notebook source
//Mount Azure storage using dbutils secret scope
dbutils.fs.mount(
  source = "wasbs://data@stavalswesteurope.blob.core.windows.net",
  mountPoint = "/mnt/hotel-weather",
  extraConfigs = Map("fs.azure.sas.data.stavalswesteurope.blob.core.windows.net" -> dbutils.secrets.get(scope="sparkstreaming", key="storagekey")))

// COMMAND ----------

//read parquet schema from mounted directory
val schema = spark.read.parquet("/mnt/hotel-weather").schema

// COMMAND ----------

val checkpoint_path = "/tmp/delta/hotel_data/_checkpoints"
val write_path = "/tmp/delta/hotel_data"

// Set up the stream to begin reading incoming files from the
// upload_path location.
val df = spark.readStream.format("parquet")
  .schema(schema)
  .load("/mnt/hotel-weather")

// Start the stream.
// Use the checkpoint_path location to keep a record of all files that
// have already been uploaded to the upload_path location.
// For those that have been uploaded since the last check,
// write the newly-uploaded files' data to the write_path location.
df.writeStream.format("delta")
  .option("checkpointLocation", checkpoint_path)
  .start(write_path)

// COMMAND ----------

//Read data from write_path location
val df_hotel_weather = spark.read.format("delta").load(write_path)

display(df_hotel_weather)

//Create view of the Dataframe
df_hotel_weather.createOrReplaceTempView("hotel_weather")


// COMMAND ----------

//Get Dataframe with avg, max, min temperature and distinct amount of hotels by city and day 
val hotelsByDayAndCity = spark.sql("""SELECT city, 
                                             wthr_date, 
                                             count(distinct(name, wthr_date)) AS distinct_hotels_amount,
                                             AVG(avg_tmpr_c) as avg_tmpr,
                                             MAX(avg_tmpr_c) as max_tmpr,
                                             MIN(avg_tmpr_c) as min_tmpr
                                      FROM hotel_weather
                                      GROUP BY city, wthr_date 
                                      ORDER BY distinct_hotels_amount DESC
                                   """)
display(hotelsByDayAndCity)

//Create view of the Dataframe
hotelsByDayAndCity.createOrReplaceTempView("hotels_by_day_and_city")

// COMMAND ----------

//Get top 10 biggest cities
val allHotelsAmount = spark.sql("""SELECT city, 
                                          count(distinct(name)) AS all_distinct_hotels
                                   FROM hotel_weather 
                                   GROUP BY city 
                                   ORDER BY all_distinct_hotels DESC 
                                   LIMIT 10
                                """)
                            .limit(10)
display(allHotelsAmount)

//Create view of the Dataframe
allHotelsAmount.createOrReplaceTempView("all_hotels_amount")

// COMMAND ----------

//Join Dataframe with biggest cities and avg, min, etc. data

val joinedHotels = spark.sql("""SELECT hotels_by_day_and_city.city, 
                                       hotels_by_day_and_city.wthr_date, 
                                       hotels_by_day_and_city.distinct_hotels_amount,
                                       avg_tmpr,
                                       max_tmpr,
                                       min_tmpr,
                                       dense_rank() over(ORDER BY hotels_by_day_and_city.city ASC) AS rank
                                FROM hotels_by_day_and_city
                                INNER JOIN all_hotels_amount
                                ON all_hotels_amount.city = hotels_by_day_and_city.city
                                GROUP BY hotels_by_day_and_city.city, 
                                         hotels_by_day_and_city.wthr_date, 
                                         hotels_by_day_and_city.distinct_hotels_amount,
                                         avg_tmpr,
                                         max_tmpr,
                                         min_tmpr
                             """)
display(joinedHotels)

//Create view of the Dataframe
joinedHotels.createOrReplaceTempView("joined_hotels")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 1 
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 2 
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 3
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 4
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 5
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 6
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 7
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 8
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 9
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * 
// MAGIC FROM joined_hotels 
// MAGIC WHERE rank = 10
// MAGIC ORDER BY wthr_date ASC

// COMMAND ----------

//Delete write_path directory and unmount Azure storage
dbutils.fs.rm(write_path, true)
dbutils.fs.unmount("/mnt/hotel-weather")

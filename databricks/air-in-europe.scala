// Databricks notebook source
// MAGIC %md
// MAGIC ## Configuring Kafka Consumer Connection

// COMMAND ----------

val topic = "air-in-europe"
val primaryConnectionString = dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "primaryConnectionString")
val jaasConfigString = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\"password=\"" + primaryConnectionString + "\";"
val bootstrapServers = primaryConnectionString.split(";")(0).split("//")(1).split("/")(0) + dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "kafkaPort")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Streaming Data from Kafka into Spark

// COMMAND ----------


import org.apache.spark.sql.functions._

val kafkaStream = spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", bootstrapServers)
                 .option("kafka.security.protocol", "SASL_SSL")
                 .option("kafka.sasl.mechanism", "PLAIN")
                 .option("kafka.sasl.jaas.config", jaasConfigString)
                 .option("subscribe", topic) // "topic" to subscribe to
                 .option("startingOffsets", "earliest") // NOTE: you can use this option to always start the stream from the beginning, or omit the option to only receive messages after connecting
                 .load

val raw_data = kafkaStream.selectExpr("CAST(key AS STRING)", "timestamp", "CAST(value AS STRING)")



// COMMAND ----------

// MAGIC %md
// MAGIC ## Defining Schemas for Pollution and Weather Data Processing

// COMMAND ----------

import org.apache.spark.sql.types._

val mainSchema = StructType(Seq(
  StructField("aqi", IntegerType)
))

val componentsSchema = StructType(Seq(
  StructField("co", DoubleType),
  StructField("no", DoubleType),
  StructField("no2", DoubleType),
  StructField("o3", DoubleType),
  StructField("so2", DoubleType),
  StructField("pm2_5", DoubleType),
  StructField("pm10", DoubleType),
  StructField("nh3", DoubleType)
))

val coordSchema = StructType(Seq(
  StructField("lon", DoubleType),
  StructField("lat", DoubleType)
))

val listItemSchema = StructType(Seq(
  StructField("main", mainSchema),
  StructField("components", componentsSchema),
  StructField("dt", LongType)
))

val listSchema = ArrayType(listItemSchema)

val pollutionSchema = StructType(Seq(
  StructField("coord", coordSchema),
  StructField("list", listSchema)
))

val weatherConditionsSchema = ArrayType(StructType(Seq(
  StructField("id", IntegerType),
  StructField("main", StringType),
  StructField("description", StringType),
  StructField("icon", StringType)
)))

val mainWeatherSchema = StructType(Seq(
  StructField("temp", DoubleType),
  StructField("feels_like", DoubleType),
  StructField("temp_min", DoubleType),
  StructField("temp_max", DoubleType),
  StructField("pressure", IntegerType),
  StructField("humidity", IntegerType)
))

val windSchema = StructType(Seq(
  StructField("speed", DoubleType),
  StructField("deg", IntegerType)
))

val cloudsSchema = StructType(Seq(
  StructField("all", IntegerType)
))

val sysSchema = StructType(Seq(
  StructField("type", IntegerType),
  StructField("id", IntegerType),
  StructField("country", StringType),
  StructField("sunrise", LongType),
  StructField("sunset", LongType)
))

val weatherSchema = StructType(Seq(
  StructField("coord", coordSchema),
  StructField("weather", weatherConditionsSchema),
  StructField("base", StringType),
  StructField("main", mainWeatherSchema),
  StructField("visibility", IntegerType),
  StructField("wind", windSchema),
  StructField("clouds", cloudsSchema),
  StructField("dt", LongType),
  StructField("sys", sysSchema),
  StructField("timezone", IntegerType),
  StructField("name", StringType)
))

val jsonSchema = StructType(Seq(
  StructField("pollution", pollutionSchema),
  StructField("weather", weatherSchema)
))


// COMMAND ----------

// MAGIC %md
// MAGIC ## Processing and Extracting Key Data from JSON Pollution and Weather Streams

// COMMAND ----------

val dfProcessed = raw_data
  .withColumn("jsonData", from_json(col("value"), jsonSchema)) // Parse the JSON string into structured data
  .withColumn("firstListItem", explode(col("jsonData.pollution.list"))) // Explode the list array to handle multiple items 
  .select(
    col("timestamp"),
    col("key").alias("country"),
    col("jsonData.weather.name").alias("location_name"),
    col("jsonData.pollution.coord.lon").alias("Lon"),
    col("jsonData.pollution.coord.lat").alias("Lat"),
    col("firstListItem.components.co").alias("CO"),
    col("firstListItem.components.no").alias("NO"),
    col("firstListItem.components.no2").alias("NO2"),
    col("firstListItem.components.o3").alias("O3"),
    col("firstListItem.components.so2").alias("SO2"),
    col("firstListItem.components.pm2_5").alias("PM2_5"),
    col("firstListItem.components.pm10").alias("PM10"),
    col("firstListItem.components.nh3").alias("NH3"),
    col("firstListItem.dt").alias("pollution_dt"),
    col("jsonData.weather.coord.lon").alias("weather_lon"),
    col("jsonData.weather.coord.lat").alias("weather_lat"),
    col("jsonData.weather.weather")(0)("description").alias("weather_description"),
    col("jsonData.weather.main.temp").alias("temperature"),
    col("jsonData.weather.main.pressure").alias("pressure"),
    col("jsonData.weather.main.humidity").alias("humidity"),
    col("jsonData.weather.visibility").alias("visibility"),
    col("jsonData.weather.wind.speed").alias("wind_speed"),
    col("jsonData.weather.clouds.all").alias("cloudiness"),
    col("jsonData.weather.sys.country").alias("sys_country"),
    col("jsonData.weather.sys.sunrise").alias("sunrise"),
    col("jsonData.weather.sys.sunset").alias("sunset"),
    col("jsonData.weather.timezone").alias("timezone"),
    )



// COMMAND ----------

// MAGIC %md
// MAGIC ## Streaming Processed Data to a Delta Table

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

val pathCore = dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "pathCore")

val deltaTableExtractedDataCheckpoint  = s"dbfs:/$pathCore/delta_air_in_europe_chk"

dfProcessed.writeStream
      .format("delta")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", deltaTableExtractedDataCheckpoint)
      .table("air_in_europe") // Make it a *managed* table...



// COMMAND ----------

// MAGIC %md
// MAGIC ## Getting Data From the Delta Table

// COMMAND ----------

val dfDeltaTable = spark.sql("SELECT * FROM air_in_europe WHERE country not in ('DZ', 'IQ', 'TN')")
dfDeltaTable.createOrReplaceTempView("delta_table")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Loading and Preparing ISO Country Codes for Data Enrichment

// COMMAND ----------

def dfCountriesIso =  spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .option("quote", "\"")
    .csv(s"dbfs:/$pathCore/csv/countries_iso.csv")
    .select("Code", "Country")

dfCountriesIso.createOrReplaceTempView("CountriesIso")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Countries With the Highest Levels of PM10 pollution.

// COMMAND ----------

val dfTopCountries = spark.sql(
  """
  SELECT c.country, ROUND(AVG(PM10), 1) AS PM10
  FROM delta_table d
  INNER JOIN CountriesIso c ON c.Code = d.Country
  GROUP BY c.country
  ORDER BY 2 DESC
  LIMIT 10
  """
)

// COMMAND ----------

// MAGIC %md
// MAGIC ##  Correlation Between Weather Conditions and Air Pollution Levels
// MAGIC The Pearson correlation coefficient measures the linear correlation between two variables, giving a value between -1 and 1:
// MAGIC
// MAGIC - **1** means there's a perfect positive linear relationship: as temperature increases, O3 levels also increase.
// MAGIC - **-1** means there's a perfect negative linear relationship: as temperature increases, O3 levels decrease.
// MAGIC - **0** means there is no linear relationship between the variables.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val corrSchema = StructType(List(
  StructField("weatherFactor", StringType, nullable = false),
  StructField("pollutionFactor", StringType, nullable = false),
  StructField("pearson", DoubleType, nullable = false)
))

val corrData = Seq(
  Row("temperature", "O3", dfDeltaTable.stat.corr("temperature", "O3", "pearson")),
  Row("temperature", "PM10", dfDeltaTable.stat.corr("temperature", "PM10", "pearson")),
  Row("temperature", "CO", dfDeltaTable.stat.corr("temperature", "CO", "pearson")),
  Row("temperature", "NO", dfDeltaTable.stat.corr("temperature", "NO", "pearson")),
  Row("temperature", "NO2", dfDeltaTable.stat.corr("temperature", "NO2", "pearson")),
  Row("temperature", "SO2", dfDeltaTable.stat.corr("temperature", "SO2", "pearson")),
  Row("temperature", "PM2_5", dfDeltaTable.stat.corr("temperature", "PM2_5", "pearson")),

  Row("pressure", "PM10", dfDeltaTable.stat.corr("pressure", "PM10", "pearson")),
  Row("pressure", "CO", dfDeltaTable.stat.corr("pressure", "CO", "pearson")),
  Row("pressure", "NO", dfDeltaTable.stat.corr("pressure", "NO", "pearson")),
  Row("pressure", "NO2", dfDeltaTable.stat.corr("pressure", "NO2", "pearson")),
  Row("pressure", "SO2", dfDeltaTable.stat.corr("pressure", "SO2", "pearson")),
  Row("pressure", "PM2_5", dfDeltaTable.stat.corr("pressure", "PM2_5", "pearson")),
  
  Row("humidity", "PM10", dfDeltaTable.stat.corr("humidity", "PM10", "pearson")),
  Row("humidity", "CO", dfDeltaTable.stat.corr("humidity", "CO", "pearson")),
  Row("humidity", "NO", dfDeltaTable.stat.corr("humidity", "NO", "pearson")),
  Row("humidity", "NO2", dfDeltaTable.stat.corr("humidity", "NO2", "pearson")),
  Row("humidity", "SO2", dfDeltaTable.stat.corr("humidity", "SO2", "pearson")),
  Row("humidity", "PM2_5", dfDeltaTable.stat.corr("humidity", "PM2_5", "pearson")), 
    
  Row("visibility", "PM10", dfDeltaTable.stat.corr("visibility", "PM10", "pearson")),
  Row("visibility", "CO", dfDeltaTable.stat.corr("visibility", "CO", "pearson")),
  Row("visibility", "NO", dfDeltaTable.stat.corr("visibility", "NO", "pearson")),
  Row("visibility", "NO2", dfDeltaTable.stat.corr("visibility", "NO2", "pearson")),
  Row("visibility", "SO2", dfDeltaTable.stat.corr("visibility", "SO2", "pearson")),
  Row("visibility", "PM2_5", dfDeltaTable.stat.corr("visibility", "PM2_5", "pearson")),

  Row("wind_speed", "PM10", dfDeltaTable.stat.corr("wind_speed", "PM10", "pearson")),
  Row("wind_speed", "CO", dfDeltaTable.stat.corr("wind_speed", "CO", "pearson")),
  Row("wind_speed", "NO", dfDeltaTable.stat.corr("wind_speed", "NO", "pearson")),
  Row("wind_speed", "NO2", dfDeltaTable.stat.corr("wind_speed", "NO2", "pearson")),
  Row("wind_speed", "SO2", dfDeltaTable.stat.corr("wind_speed", "SO2", "pearson")),
  Row("wind_speed", "PM2_5", dfDeltaTable.stat.corr("wind_speed", "PM2_5", "pearson")), 
  
  Row("cloudiness", "PM10", dfDeltaTable.stat.corr("cloudiness", "PM10", "pearson")),
  Row("cloudiness", "CO", dfDeltaTable.stat.corr("cloudiness", "CO", "pearson")),
  Row("cloudiness", "NO", dfDeltaTable.stat.corr("cloudiness", "NO", "pearson")),
  Row("cloudiness", "NO2", dfDeltaTable.stat.corr("cloudiness", "NO2", "pearson")),
  Row("cloudiness", "SO2", dfDeltaTable.stat.corr("cloudiness", "SO2", "pearson")),
  Row("cloudiness", "PM2_5", dfDeltaTable.stat.corr("cloudiness", "PM2_5", "pearson")),   
)

val dfCorrelations = spark.createDataFrame(spark.sparkContext.parallelize(corrData), corrSchema)


// COMMAND ----------

// MAGIC %md
// MAGIC ### Capital Cities With the Highest Levels of Traffic Polution.

// COMMAND ----------

def dfCapitals =  spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .option("quote", "\"")
    .csv(s"dbfs:/$pathCore/csv/europe_capitals.csv")
    .select("capital","capital_lat", "capital_lng")

dfCapitals.createOrReplaceTempView("capitals")

val dfCapitalsPollution = spark.sql(
  """
    SELECT c.capital, ROUND(AVG(d.NO2), 1) AS NO2
    FROM delta_table d
    INNER JOIN Capitals c ON 
                              ((d.Lat between (c.capital_lat - 0.1) and (c.capital_lat + 0.1)) AND
                               (d.Lon between (c.capital_lng - 0.1) and (c.capital_lng + 0.1))
                              )
    GROUP BY c.capital
    ORDER BY 2 DESC
    LIMIT 10
  """

)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribution of Specific Pollutants Across Europe

// COMMAND ----------

val dfWithRoundedCoordinates = dfDeltaTable
  .withColumn("lat_int", round($"lat", 1)) // Round to 1 decimal place
  .withColumn("lon_int", round($"lon", 1))
  .select("lat_int", "lon_int", "PM10")
  .groupBy($"lat_int", $"lon_int")
  .agg(round(avg("PM10"), 1).alias("avg_PM10"))
  .orderBy($"lat_int", $"lon_int")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Loading to the Service Layer (Azure SQL Database)
// MAGIC ### Set the connection

// COMMAND ----------

import java.util.Properties

val jdbcUrl = dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "azureSqlDatabaseUrl")

val connectionProperties = new Properties()
connectionProperties.setProperty("user", dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "azureSqlDatabaseUser"))
connectionProperties.setProperty("password", dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "azureSqlDatabasePassword"))
connectionProperties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")



// COMMAND ----------

// MAGIC %md
// MAGIC ### Name the tables

// COMMAND ----------

val highPollutionTable = "HighPollution"
val correlationTable = "Correlation"
val capitalPollutionTable = "CapitalPollution"
val distributionTable = "Distribution"


// COMMAND ----------

// MAGIC %md
// MAGIC ### Clear the tables

// COMMAND ----------

import java.sql.DriverManager
import java.util.Properties

val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)

try {
  val statement = connection.createStatement()

  statement.execute(s"TRUNCATE TABLE $highPollutionTable")
  statement.execute(s"TRUNCATE TABLE $correlationTable")
  statement.execute(s"TRUNCATE TABLE $capitalPollutionTable")
  statement.execute(s"TRUNCATE TABLE $distributionTable")
} catch {
  case e: Exception => e.printStackTrace()
} finally {
  connection.close()
}



// COMMAND ----------

// MAGIC %md
// MAGIC ### Load data to the service layer (Azure SQL Database)
// MAGIC

// COMMAND ----------

dfTopCountries.write
  .mode("append")
  .jdbc(jdbcUrl, highPollutionTable, connectionProperties)

dfCorrelations.write
  .mode("append")
  .jdbc(jdbcUrl, correlationTable, connectionProperties)

dfCapitalsPollution.write
  .mode("append")
  .jdbc(jdbcUrl, capitalPollutionTable, connectionProperties)  

dfWithRoundedCoordinates.write
  .mode("append")
  .jdbc(jdbcUrl, distributionTable, connectionProperties)  

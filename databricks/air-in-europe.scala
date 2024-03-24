// Databricks notebook source
val topic = "air-in-europe"
val primaryConnectionString = dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "primaryConnectionString")
val jaasConfigString = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\"password=\"" + primaryConnectionString + "\";"
val bootstrapServers = primaryConnectionString.split(";")(0).split("//")(1).split("/")(0) + dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "kafkaPort")

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

// Select the columns of interest from the streaming DataFrame
val raw_data = kafkaStream.selectExpr("CAST(key AS STRING)", "timestamp", "CAST(value AS STRING)")



// COMMAND ----------

// MAGIC %md
// MAGIC Pollution schema

// COMMAND ----------

import org.apache.spark.sql.types._

// Define the schema for the main pollution info
val mainSchema = StructType(Seq(
  StructField("aqi", IntegerType)
))

// Define the schema for the pollution components
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

// Define the schema for the coordinates (used in both pollution and weather)
val coordSchema = StructType(Seq(
  StructField("lon", DoubleType),
  StructField("lat", DoubleType)
))

// Define the schema for each item in the pollution list
val listItemSchema = StructType(Seq(
  StructField("main", mainSchema),
  StructField("components", componentsSchema),
  StructField("dt", LongType)
))

// Define the schema for the list array within pollution
val listSchema = ArrayType(listItemSchema)

// Define the schema for the pollution information
val pollutionSchema = StructType(Seq(
  StructField("coord", coordSchema),
  StructField("list", listSchema)
))

// Weather schema definition starts here
// Define the schema for the weather conditions
val weatherConditionsSchema = ArrayType(StructType(Seq(
  StructField("id", IntegerType),
  StructField("main", StringType),
  StructField("description", StringType),
  StructField("icon", StringType)
)))

// Define the schema for the main weather data
val mainWeatherSchema = StructType(Seq(
  StructField("temp", DoubleType),
  StructField("feels_like", DoubleType),
  StructField("temp_min", DoubleType),
  StructField("temp_max", DoubleType),
  StructField("pressure", IntegerType),
  StructField("humidity", IntegerType)
))

// Define the schema for the wind
val windSchema = StructType(Seq(
  StructField("speed", DoubleType),
  StructField("deg", IntegerType)
))

// Define the schema for the clouds
val cloudsSchema = StructType(Seq(
  StructField("all", IntegerType)
))

// Define the schema for the system information
val sysSchema = StructType(Seq(
  StructField("type", IntegerType),
  StructField("id", IntegerType),
  StructField("country", StringType),
  StructField("sunrise", LongType),
  StructField("sunset", LongType)
))

// Define the complete schema for the weather information
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

// Define the complete JSON schema that includes both pollution and weather
val jsonSchema = StructType(Seq(
  StructField("pollution", pollutionSchema),
  StructField("weather", weatherSchema)
))


// COMMAND ----------

// Assuming df is your initial DataFrame with the 'value' column containing the JSON strings
val dfProcessed = raw_data
  .withColumn("jsonData", from_json(col("value"), jsonSchema)) // Parse the JSON string into structured data
  .withColumn("firstListItem", explode(col("jsonData.pollution.list"))) // Explode the list array to handle multiple items if necessary
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

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

val pathCore = dbutils.secrets.get(scope = "AIR_IN_EUROPE_SCOPE", key = "pathCore")

val deltaTableExtractedDataCheckpoint  = s"dbfs:/$pathCore/delta_air_in_europe_chk"

dfProcessed.writeStream   // The same stream we're getting from "Kafka", transforming and displaying above...
      .format("delta")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", deltaTableExtractedDataCheckpoint)
      .table("air_in_europe") // Make it a *managed* table...



// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) FROM air_in_europe

// COMMAND ----------

val dfDeltaTable = spark.sql("SELECT * FROM air_in_europe WHERE country not in ('DZ', 'IQ', 'TN')")
dfDeltaTable.createOrReplaceTempView("delta_table")

// COMMAND ----------

def dfCountriesIso =  spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .option("quote", "\"")
    .csv(s"dbfs:/$pathCore/csv/countries_iso.csv")
    .select("Code", "Country")

dfCountriesIso.createOrReplaceTempView("CountriesIso")
// display(dfCountriesIso)

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

display(dfTopCountries)


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

// Calculate Pearson correlation coefficient
val corrTemperatureO3 = dfDeltaTable.stat.corr("temperature", "O3", "pearson")
val corrTemperaturePM10 = dfDeltaTable.stat.corr("temperature", "PM10", "pearson")
val corrVisibilityPM10 = dfDeltaTable.stat.corr("visibility", "PM10", "pearson")
val corrWindPM10 = dfDeltaTable.stat.corr("wind_speed", "PM10", "pearson")
val corrCloudinessO3 = dfDeltaTable.stat.corr("cloudiness", "O3", "pearson")

val corrSchema = StructType(List(
  StructField("weatherFactor", StringType, nullable = false),
  StructField("pollutionFactor", StringType, nullable = false),
  StructField("pearson", DoubleType, nullable = false)
))

val corrData = Seq(
  Row("temperature", "O3", corrTemperatureO3),
  Row("temperature", "PM10", corrTemperaturePM10),
  Row("visibility", "PM10", corrVisibilityPM10),
  Row("wind_speed", "PM10", corrWindPM10),
  Row("cloudiness", "O3", corrCloudinessO3)
)

val dfCorrelations = spark.createDataFrame(spark.sparkContext.parallelize(corrData), corrSchema)

display(dfCorrelations)

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

display(dfCapitalsPollution)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distribution of Specific Pollutants Across Europe

// COMMAND ----------

val dfWithRoundedCoordinates = dfDeltaTable
  .withColumn("lat_int", round($"lat"))
  .withColumn("lon_int", round($"lon"))
  .select("lat_int", "lon_int", "PM10")
  .groupBy($"lat_int", $"lon_int")
  .agg(round(avg("PM10"), 1).alias("avg_PM10"))
  .orderBy($"lat_int", $"lon_int")

println(dfWithRoundedCoordinates.count() )
dfWithRoundedCoordinates.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load to the Service Layer
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
  // Creating a statement object to execute the query
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

'''
Functionality tested using pyspark shell locally.  
Used Spark 2.2.0 and Python 3.6
'''

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, FloatType, StringType

filenames = "weather.*.csv"


def analyseWeather(spark):
	#set the datatypes
	weatherSchema = StructType([
		StructField("ForecastSiteCode", IntegerType, True),
		StructField("ObservationTime", IntegerType, True), 
		StructField("ObservationDate", DateType(), True),
		StructField("WindDirection", IntegerType(), True),
		StructField("WindSpeed", IntegerType(), True),
		StructField("WindGust", FloatType(), True),
		StructField("Visibility", FloatType(), True),
		StructField("ScreenTemperature", FloatType(), True),
		StructField("Pressure", FloatType(), True),
		StructField("SignificantWeatherCode", IntegerType(), True),
		StructField("SiteName", StringType(), True),
		StructField("Latitude", FloatType(), True),
		StructField("Longitude", FloatType(), True),
		StructField("Region", StringType(), True),
		StructField("Country", StringType(), True),
		]) 

	# open both csvs in a single dataframe
    df = spark.read.format("csv") /
        .option("header", "true") /
        .schema(weatherSchema) /
        .load("weather.*.csv")

    # save data to parquet file
    df.write.parquet("weather.parquet")

    # read parquet-format dataframe
    weather_df = spark.read.parquet("weather.parquet")

    # create temp view for running sql queries
    weather = weather_df.getOrReplaceTempView("weather")
    
    # hottest day 
    hottestday = spark.sql("SELECT ObservationDate FROM weather WHERE ScreenTemperature == MAX(ScreenTemperature").collect()[0].asDict()['hottestday']

    # maximum temperature
    maxtemp = spark.sql("SELECT MAX(ScreenTemperature) as maxtemp FROM weather").collect()[0].asDict()['maxtemp']

    # hottest region
    hottestregion = spark.sql("SELECT Region FROM weather WHERE ScreenTemperature == MAX(ScreenTemperature").collect()[0].asDict()['hottestregion']
	

if __name__ == "__main__":

	# create spark session
	spark = SparkSession.builder /
        .master("local") /
        .appName("parquet_weather") /
        .getOrCreate()

    # run function    
    analyseWeather(spark)

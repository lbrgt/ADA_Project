from pyspark.sql import *
import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()

    EVENTS_SCHEMA = StructType([
        StructField("GLOBALEVENTID", LongType(), True),
        StructField("Day_DATE", StringType(), True),
        StructField("MonthYear_Date", StringType(), True),
        StructField("Year_Date", StringType(), True),
        StructField("FractionDate", FloatType(), True),
        StructField("Actor1Code", StringType(), True),
        StructField("Actor1Name", StringType(), True),
        StructField("Actor1CountryCode", StringType(), True),
        StructField("Actor1KnownGroupCode", StringType(), True),
        StructField("Actor1EthnicCode", StringType(), True),
        StructField("Actor1Religion1Code", StringType(), True),
        StructField("Actor1Religion2Code", StringType(), True),
        StructField("Actor1Type1Code", StringType(), True),
        StructField("Actor1Type2Code", StringType(), True),
        StructField("Actor1Type3Code", StringType(), True),
        StructField("Actor2Code", StringType(), True),
        StructField("Actor2Name", StringType(), True),
        StructField("Actor2CountryCode", StringType(), True),
        StructField("Actor2KnownGroupCode", StringType(), True),
        StructField("Actor2EthnicCode", StringType(), True),
        StructField("Actor2Religion1Code", StringType(), True),
        StructField("Actor2Religion2Code", StringType(), True),
        StructField("Actor2Type1Code", StringType(), True),
        StructField("Actor2Type2Code", StringType(), True),
        StructField("Actor2Type3Code", StringType(), True),
        StructField("IsRootEvent", LongType(), True),
        StructField("EventCode", StringType(), True),
        StructField("EventBaseCode", StringType(), True),
        StructField("EventRootCode", StringType(), True),
        StructField("QuadClass", LongType(), True),
        StructField("GoldsteinScale", FloatType(), True),
        StructField("NumMentions", LongType(), True),
        StructField("NumSources", LongType(), True),
        StructField("NumArticles", LongType(), True),
        StructField("AvgTone", FloatType(), True),
        StructField("Actor1Geo_Type", LongType(), True),
        StructField("Actor1Geo_FullName", StringType(), True),
        StructField("Actor1Geo_CountryCode", StringType(), True),
        StructField("Actor1Geo_ADM1Code", StringType(), True),
        StructField("Actor1Geo_ADM2Code", StringType(), True),
        StructField("Actor1Geo_Lat", FloatType(), True),
        StructField("Actor1Geo_Long", FloatType(), True),
        StructField("Actor1Geo_FeatureID", StringType(), True),
        StructField("Actor2Geo_Type", LongType(), True),
        StructField("Actor2Geo_FullName", StringType(), True),
        StructField("Actor2Geo_CountryCode", StringType(), True),
        StructField("Actor2Geo_ADM1Code", StringType(), True),
        StructField("Actor2Geo_ADM2Code", StringType(), True),
        StructField("Actor2Geo_Lat", FloatType(), True),
        StructField("Actor2Geo_Long", FloatType(), True),
        StructField("Actor2Geo_FeatureID", StringType(), True),
        StructField("ActionGeo_Type", LongType(), True),
        StructField("ActionGeo_FullName", StringType(), True),
        StructField("ActionGeo_CountryCode", StringType(), True),
        StructField("ActionGeo_ADM1Code", StringType(), True),
        StructField("ActionGeo_ADM2Code", StringType(), True),
        StructField("ActionGeo_Lat", FloatType(), True),
        StructField("ActionGeo_Long", FloatType(), True),
        StructField("ActionGeo_FeatureID", StringType(), True),
        StructField("DATEADDED", LongType(), True),
        StructField("SOURCEURL", StringType(), True)
    ])

    event_df_example = spark.read.option("sep", "\t").csv(sys.argv[1], schema=EVENTS_SCHEMA)
    event_df_example.show(1)

    event_df_example.write.parquet('one_event_example', mode='overwrite')
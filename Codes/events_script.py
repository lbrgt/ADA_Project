from pyspark.sql import *
from pyspark.sql.types import *
import os

spark = SparkSession.builder.getOrCreate()

EVENTS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("Day_DATE",StringType(),True),
    StructField("MonthYear_Date",StringType(),True),
    StructField("Year_Date",StringType(),True),
    StructField("FractionDate",FloatType(),True),
    StructField("Actor1Code",StringType(),True),
    StructField("Actor1Name",StringType(),True),
    StructField("Actor1CountryCode",StringType(),True),
    StructField("Actor1KnownGroupCode",StringType(),True),
    StructField("Actor1EthnicCode",StringType(),True),
    StructField("Actor1Religion1Code",StringType(),True),
    StructField("Actor1Religion2Code",StringType(),True),
    StructField("Actor1Type1Code",StringType(),True),
    StructField("Actor1Type2Code",StringType(),True),
    StructField("Actor1Type3Code",StringType(),True),
    StructField("Actor2Code",StringType(),True),
    StructField("Actor2Name",StringType(),True),
    StructField("Actor2CountryCode",StringType(),True),
    StructField("Actor2KnownGroupCode",StringType(),True),
    StructField("Actor2EthnicCode",StringType(),True),
    StructField("Actor2Religion1Code",StringType(),True),
    StructField("Actor2Religion2Code",StringType(),True),
    StructField("Actor2Type1Code",StringType(),True),
    StructField("Actor2Type2Code",StringType(),True),
    StructField("Actor2Type3Code",StringType(),True),
    StructField("IsRootEvent",LongType(),True),
    StructField("EventCode",StringType(),True),
    StructField("EventBaseCode",StringType(),True),
    StructField("EventRootCode",StringType(),True),
    StructField("QuadClass",LongType(),True),
    StructField("GoldsteinScale",FloatType(),True),
    StructField("NumMentions",LongType(),True),
    StructField("NumSources",LongType(),True),
    StructField("NumArticles",LongType(),True),
    StructField("AvgTone",FloatType(),True),
    StructField("Actor1Geo_Type",LongType(),True),
    StructField("Actor1Geo_FullName",StringType(),True),
    StructField("Actor1Geo_CountryCode",StringType(),True),
    StructField("Actor1Geo_ADM1Code",StringType(),True),
    StructField("Actor1Geo_ADM2Code",StringType(),True),
    StructField("Actor1Geo_Lat",FloatType(),True),
    StructField("Actor1Geo_Long",FloatType(),True),
    StructField("Actor1Geo_FeatureID",StringType(),True),
    StructField("Actor2Geo_Type",LongType(),True),
    StructField("Actor2Geo_FullName",StringType(),True),
    StructField("Actor2Geo_CountryCode",StringType(),True),
    StructField("Actor2Geo_ADM1Code",StringType(),True),
    StructField("Actor2Geo_ADM2Code",StringType(),True),
    StructField("Actor2Geo_Lat",FloatType(),True),
    StructField("Actor2Geo_Long",FloatType(),True),
    StructField("Actor2Geo_FeatureID",StringType(),True),
    StructField("ActionGeo_Type",LongType(),True),
    StructField("ActionGeo_FullName",StringType(),True),
    StructField("ActionGeo_CountryCode",StringType(),True),
    StructField("ActionGeo_ADM1Code",StringType(),True),
    StructField("ActionGeo_ADM2Code",StringType(),True),
    StructField("ActionGeo_Lat",FloatType(),True),
    StructField("ActionGeo_Long",FloatType(),True),
    StructField("ActionGeo_FeatureID",StringType(),True),
    StructField("DATEADDED",LongType(),True),
    StructField("SOURCEURL",StringType(),True)
    ])

REDUCED_EVENT_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("Day_DATE",StringType(),True),
    StructField("Actor1Code",StringType(),True),
    StructField("Actor1Name",StringType(),True),
    StructField("EventRootCode",StringType(),True),
    StructField("GoldsteinScale",FloatType(),True),
    StructField("NumMentions",LongType(),True),
    StructField("NumSources",LongType(),True),
    StructField("NumArticles",LongType(),True),
    StructField("AvgTone",FloatType(),True)
    ])

DATA_DIR = "hdfs:///datasets/gdeltv2"
events_df = spark.read.csv(os.path.join(DATA_DIR, "*.export.CSV"))
events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)

event_dates = events_df.select("MonthYear_Date").withColumn("MonthYear_Date", events_df['MonthYear_Date']) \
                       .sort('MonthYear_Date', ascending=True )

event_dates.show(30)

#not_english_gkg_df = gkg_df.filter(gkg_df.TranslationInfo.isNotNull())
#print(not_english_gkg_df.count())
#not_english_mentions_df = mentions_df.filter(mentions_df.MentionDocTranslationInfo.isNull())
#print("MENTIONS:", mentions_df.count())
#print("NOT ENG MENTIONS", not_english_mentions_df.count())

#mentions_not_english = mentions_df.select("MentionDocTranslationInfo")
#print('AA')
#mentions_not_english.count()
#mentions_not_english.write.parquet('mentions_not_english.parquet', mode='overwrite')

#dates_year = a.select("Year_Date").withColumn("Year_Date", a['Year_Date']
              #.cast(LongType())).sort('Year_Date', ascending=True )




#Create an empty dataframe
#df = spark.createDataFrame([], REDUCED_EVENT_SCHEMA)

#import subprocess
#for file_path in l_file :
 #   df1 = spark.read.option("sep", "\t").csv(file_path, schema=EVENTS_SCHEMA)
 #   df1 = df1.select("GLOBALEVENTID", "Day_DATE", "Actor1Code", "Actor1Name", "EventRootCode", "GoldsteinScale",
  #                   "NumMentions", "NumSources", "NumArticles", "AvgTone"
   #                  )
    #df = df1.union(df)
    #print('A!!')

#df.write.parquet('all_events_parquet', mode='overwrite')

#pattern = "export"
#args = "hadoop fs -ls /datasets/gdeltv2 | awk '{print $8}' | grep "+pattern
#p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,shell=True)

#s_output, s_err = p.communicate()
#l_file = s_output.split('\n')




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

MENTIONS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("EventTimeDate",LongType(),True),
    StructField("MentionTimeDate",LongType(),True),
    StructField("MentionType",LongType(),True),
    StructField("MentionSourceName",StringType(),True),
    StructField("MentionIdentifier",StringType(),True),
    StructField("SentenceID",LongType(),True),
    StructField("Actor1CharOffset",LongType(),True),
    StructField("Actor2CharOffset",LongType(),True),
    StructField("ActionCharOffset",LongType(),True),
    StructField("InRawText",LongType(),True),
    StructField("Confidence",LongType(),True),
    StructField("MentionDocLen",LongType(),True),
    StructField("MentionDocTone",FloatType(),True),
    StructField("MentionDocTranslationInfo",StringType(),True),
    StructField("Extras",StringType(),True)
    ])

GKG_SCHEMA2 = StructType([StructField("GKGRECORDID",StringType(),True)])

GKG_SCHEMA = StructType([
        StructField("GKGRECORDID",StringType(),True),
        StructField("DATE",StringType(),True),
        #StructField("DATE",TimestampType(),True),
        StructField("SourceCollectionIdentifier",StringType(),True),
        StructField("SourceCommonName",StringType(),True),
        StructField("DocumentIdentifier",StringType(),True),
        StructField("Counts",StringType(),True),
        StructField("V2Counts",StringType(),True),
        StructField("Themes",StringType(),True),
        StructField("V2Themes",StringType(),True),
        StructField("Locations",StringType(),True),
        StructField("V2Locations",StringType(),True),
        StructField("Persons",StringType(),True),
        StructField("V2Persons",StringType(),True),
        StructField("Organizations",StringType(),True),
        StructField("V2Organizations",StringType(),True),
        StructField("V2Tone",StringType(),True),
        StructField("Dates",StringType(),True),
        StructField("GCAM",StringType(),True),
        StructField("SharingImage",StringType(),True),
        StructField("RelatedImages",StringType(),True),
        StructField("SocialImageEmbeds",StringType(),True),
        StructField("SocialVideoEmbeds",StringType(),True),
        StructField("Quotations",StringType(),True),
        StructField("AllNames",StringType(),True),
        StructField("Amounts",StringType(),True),
        StructField("TranslationInfo",StringType(),True),
        StructField("Extras",StringType(),True)
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

#events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)
#mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)
gkg_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.gkg.csv"),schema=GKG_SCHEMA)

gkg_df = gkg_df.select("GKGRECORDID").groupBy("GKGRECORDID").count()
gkg_df.show()

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


#a = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.gkg.csv"),schema=GKG_SCHEMA)
#dates_year = a.select("DATE").withColumn("DATE", a['DATE']) \
             # .sort('DATE', ascending=True )
#dates_year.show(1)
#a.write.parquet('all_events_parquet', mode='overwrite')

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
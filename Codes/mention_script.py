from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

spark = SparkSession.builder.getOrCreate()

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

DATA_DIR = "hdfs:///datasets/gdeltv2"
mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)

mentions_df = mentions_df.withColumn("Year-Month-Day", to_date(concat(col("MentionTimeDate")), "yyyyMMddhhmmss"))

mentions_df = mentions_df.select('MentionDocTone', 'MentionTimeDate', 'MentionSourceName', 'Year-Month-Day', \
                                  year(mentions_df["Year-Month-Day"]).alias('Year'), \
                                  month(mentions_df["Year-Month-Day"]).alias('Month'),
                                )\
                         .withColumn("Year-Month",concat(col("Year"), lit("-"), col("Month"))) \
                         .drop('Year', 'Month')

mentions_df.write.parquet('mentions_casted_date.parquet', mode='overwrite')
#season_intervals.show()


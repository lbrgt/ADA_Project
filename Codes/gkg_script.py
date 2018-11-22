from pyspark.sql import *
from pyspark.sql.types import *
import os
spark = SparkSession.builder.getOrCreate()

GKG_SCHEMA2 = StructType([StructField("GKGRECORDID",StringType(),True)])

GKG_SCHEMA = StructType([
        StructField("GKGRECORDID",StringType(),True),
        StructField("DATE",StringType(),True),
        StructField("DATE",TimestampType(),True),
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

DATA_DIR = "hdfs:///datasets/gdeltv2"
gkg_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.gkg.csv"),schema=GKG_SCHEMA)

gkg_df.show()

#
#a.write.parquet('all_events_parquet', mode='overwrite')
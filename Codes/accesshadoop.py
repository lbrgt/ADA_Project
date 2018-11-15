from pyspark.sql import *
import sys

if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    posts_df = spark.read.csv(sys.argv[1])

    posts_df = posts_df.select('_c1')
    posts_df.show()

    #posts_df.show(2, truncate=False, vertical=True)
    #posts_df.printSchema()

    #cvf = posts_df.select('_c4')
    #cvf.show()
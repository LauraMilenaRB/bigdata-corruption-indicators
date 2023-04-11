from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date

def main():
    spark = SparkSession.builder.appName('test').getOrCreate()

    date_data = date.today()
    data_source = spark.readStream.text(f"s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/*.snappy")
    print("read")
    query = data_source \
        .writeStream \
        .outputMode("append") \
        .format(f"parquet") \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", f"s3://test-pgr-aws-logs/") \
        .option("path", f"s3://test-pgr-raw-zone/t_streaming_contracts/{date_data}") \
        .start()

    print("read2")
    query.awaitTermination()


if __name__ == '__main__':
    main()

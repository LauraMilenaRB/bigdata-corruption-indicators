from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('test').getOrCreate()

    data_source = spark.readStream.text(f"s3://test-pgr-staging-zone/streaming_contracts/2023-03-18/*.snappy")
    print("read")
    query = data_source \
        .writeStream \
        .outputMode("append") \
        .format(f"parquet") \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", f"s3://test-pgr-staging-zone/") \
        .option("path", f"s3://test-pgr-staging-zone/test/") \
        .start()

    print("read2")
    query.awaitTermination()


if __name__ == '__main__':
    main()

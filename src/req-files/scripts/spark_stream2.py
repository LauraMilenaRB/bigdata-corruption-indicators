from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import time

def main():
    spark = SparkSession.builder.appName('test').getOrCreate()



    input = f"s3://test-pgr-staging-zone/streaming_contracts/2023-03-18/*.snappy"
    print("read")

    appName = "PythonKinesisApp"
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 1)

    streamName = 'DemoStream'
    endpointUrl = 'https://kinesis.us-east-1.amazonaws.com'
    regionName = 'us-east-1'
    AWS_ACCESS_KEY_ID = 'AKIAVFHJMDPBX43OSDVP'
    SECRET_ACCESS_KEY = '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'
    checkpointInterval = 5
    kinesisstream = KinesisUtils.createStream(ssc, appName,
                                              streamName, endpointUrl, regionName,
                                              InitialPositionInStream.LATEST,
                                              checkpointInterval,
                                              awsAccessKeyId=AWS_ACCESS_KEY_ID,
                                              awsSecretKey=SECRET_ACCESS_KEY)
    lines = kinesisstream.map(lambda x: x[1])
    print(lines)
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    time.sleep(600)  # Run stream for 10 minutes just in case no detection of producer
    # ssc.awaitTermination()
    print("read2")
    ssc.stop(stopSparkContext=True, stopGraceFully=True)


if __name__ == '__main__':
    main()

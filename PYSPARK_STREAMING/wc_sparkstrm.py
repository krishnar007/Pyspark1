from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

if __name__ == "__main__":
    sc = SparkContext(appName="PySpark Streaming", master="local[2]")
    ssc = StreamingContext(sc, 5)
    #source_path = sys.argv[1]
    # source_path = u"/Users/PycharmProjects/SPARKSQL-TEST/PYSPARK_STREAMING/data/word.txt"
    # dstream = ssc.textFileStream("file:///Users/PycharmProjects/SPARKSQL-TEST/PYSPARK_STREAMING/data/")
    dstream = ssc.socketTextStream('localhost', 9999)
    words = dstream.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    words.pprint()
    ssc.start()
    ssc.awaitTermination()

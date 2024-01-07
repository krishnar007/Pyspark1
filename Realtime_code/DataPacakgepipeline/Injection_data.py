import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession


# created  class for injection the data
class Inject:
    def __init__(self, spark):
        self.spark = spark

    # creating method for injection.

    def inject_data(self):
        print("Injection the data from the SOUCRE SYSTEM")
        # spark.read.format("csv") \
        #     .options(inferSchema="True", delimiter=",") \
        #     .load("file:////Users/krishnar/PycharmProjects/SPARKSQL-TEST/json_data/100_Sales_records.csv")

        customer_data = self.spark.read.format("csv").load("file:////Users/krishnar/PycharmProjects/SPARKSQL-TEST/json_data/DATASETS _2/customer_records.csv",header=True)
        # customer_data.show(5)
        return customer_data

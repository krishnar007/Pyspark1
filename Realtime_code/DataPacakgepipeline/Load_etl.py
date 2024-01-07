import sys

import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession


# created  class for injection the data
class Persist:
    def __init__(self, spark):
        self.spark = spark

    def load_etl(self,df):
        try:
            print(" Extrenal data load sucessfully")
            df.write.option("header", "true").mode("overwrite").csv("file:////Users/krishnar/PycharmProjects/SPARKSQL-TEST/json_data/DATASETS _2/RK_externaldata.csv")
        except Exception as exp:
            print(" An Error occured in loading process" + str(exp))
            sys.exit(1)

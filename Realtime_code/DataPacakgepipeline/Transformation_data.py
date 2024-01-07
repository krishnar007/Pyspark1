import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession


# created  class for injection the data
class transform_data_ETL:
    def __init__(self, spark):
        self.spark = spark

    def transform_ETL(self, df):
        print("Transformations started on the dataframe")
        df1=df.na.drop()
        # df1.show()
        return  df1


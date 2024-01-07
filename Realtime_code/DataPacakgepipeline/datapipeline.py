import sys

import pyspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
import Injection_data
import Transformation_data
import Load_etl
# from pyspark.shell import spark
import logging

class Pipeline_main:
    def run_pipeline(self):
        try:
            print("Running the ETL pipeline")
            ingest_etl = Injection_data.Inject(self.spark)
            df = ingest_etl.inject_data()
            # df.show()

            print("Transformation the data ")
            transform_process = Transformation_data.transform_data_ETL(self.spark)
            transform_df = transform_process.transform_ETL(df)
            transform_df.show(5)

            print(" Presisting the data run job")
            persist_process = Load_etl.Persist(self.spark)
            persist_df = persist_process.load_etl(transform_df)
            return
        except Exception as e:
            print(" An ERROR is occured" + str(e))
            sys.exit(1)
        return

    def create_spark_session(self):
        self.spark = SparkSession.builder.appName("Real time data B14").master("local").getOrCreate()


if __name__ == "__main__":
    # logging.debug("debug message")
    # logging.info("info message")
    # logging.warning("warning message")
    # logging.error("Error message")
    logging.debug("Application started")
    pipeline = Pipeline_main()
    pipeline.create_spark_session()
    logging.debug("Spark session Started")
    pipeline.run_pipeline()
    logging.debug("Pipeline completed successfully")

# from urllib.request import urlopen
# import requests
# import json
# from pyspark.sql.functions import udf, col, explode
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
# from pyspark.sql import Row
# from pyspark import SparkContext as sc
# from pyspark.sql import SparkSession
# import ssl
# import certifi
# from pyspark.shell import spark
# from urllib.request import urlopen
# import urllib
# from pyspark import SparkConf
# from pyspark import SparkContext
# from pyspark.shell import spark
# sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
# content = urlopen("https://randomuser.me/api/0.8/?results=100").read()
# rdd = sc.parallelize([content])
# df = spark.read.json(rdd,multiLine=True)
# df.show()
#
# #https://stackoverflow.com/questions/52805115/certificate-verify-failed-unable-to-get-local-issuer-certificate
# # import ssl
# # import certifi
# # from urllib.request import urlopen
# #
# # request = "https://example.com"
# # urlopen(request, context=ssl.create_default_context(cafile=certifi.where()))
#
from pyspark.sql import Row
from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
import ssl
import certifi
import requests
import json
from pyspark.shell import spark
from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen

spark = SparkSession.builder.getOrCreate()

url = 'https://randomuser.me/api/0.8/?results=100'
jsonData = urlopen(url).read().decode('utf-8')
rdd = spark.sparkContext.parallelize([jsonData])
df = spark.read.json(rdd)
df.show()
df.printSchema()

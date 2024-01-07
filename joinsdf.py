from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.shell import spark

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RDD to DF").master("local").getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("Error")
    df_emp = spark.read.format("csv") \
        .options(inferSchema="True", delimiter=",") \
        .load("file:////Users/krishnar/PycharmProjects/SPARKSQL-TEST/json_data/DATASETS _2/emp_data.csv") \
        .toDF("emp", "name", "superior_empid", "year_joined", "empdept_id", "gender", "salary")

        # .load("file://///Users/krishnar/PycharmProjects/SPARKSQL-TEST/json_data/Datasets-master/insurance.csv") \
        # .toDF("Age", "Sex", "BMI", "Children", "Smoker", "Region", "Charges")
    df_emp.show(truncate=False)
    df_emp.printSchema()

    df_dept = spark.read.format("csv") \
        .options(inferSchema="True", delimiter=",") \
        .load("file:////Users/krishnar/PycharmProjects/SPARKSQL-TEST/json_data/DATASETS _2/dept_data.csv") \
        .toDF("dept_name", "dept_id")

    df_dept.show(truncate=False)
    df_dept.printSchema()

# Inner Join method.
    df_emp.join(df_dept, df_emp.empdept_id==df_dept.dept_id, "inner").show(truncate=False)

#Letf join
    # Inner Join method.
    df_emp.join(df_dept, df_emp.empdept_id == df_dept.dept_id, "left").show(truncate=False)

#Right join
    # Inner Join method.
    df_emp.join(df_dept, df_emp.empdept_id == df_dept.dept_id, "right").show(truncate=False)
#full joins
    df_emp.join(df_dept, df_emp.empdept_id == df_dept.dept_id, "full").show(truncate=False)
    print("left semi join")
    df_emp.join(df_dept, df_emp.empdept_id == df_dept.dept_id, "leftsemi").show(truncate=False)
    print("left Anti join")
    df_emp.join(df_dept, df_emp.empdept_id == df_dept.dept_id, "leftanti").show(truncate=False)
    df_emp.createOrReplaceTempView("df_emp_sql")
    spark.sql("show tables from default").show()

    df_emp.createGlobalTempView("df_emp_sql_global")
    spark.sql("show tables from global_temp").show()






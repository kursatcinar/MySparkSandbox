from pyspark.sql import SparkSession

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .master("local[2]") \
            .appName("MySparkSandbox") \
            .config("spark.jars", "mysql-connector-j-9.0.0.jar") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()



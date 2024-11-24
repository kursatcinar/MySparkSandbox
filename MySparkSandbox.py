import sys
from lib.logger import Log4j
from lib.utils import get_spark_session

if __name__ == "__main__":
    job_run_env = sys.argv[1].upper()
    spark = get_spark_session(job_run_env)
    mysql_driver = "com.mysql.cj.jdbc.Driver"
    mysql_url = "jdbc:mysql://localhost:3307/employees"

    logger = Log4j(spark)
    logger.info("Spark session created")

    departments_df = spark.read.format("jdbc").option("driver", mysql_driver) \
        .option("url", mysql_url) \
        .option("dbtable", "departments") \
        .option("user", sys.argv[2]) \
        .option("password", sys.argv[3]) \
        .load()

    # departments_df.show(5)

    employees_df = spark.read.format("jdbc").option("driver", mysql_driver) \
        .option("url", mysql_url) \
        .option("dbtable", "employees") \
        .option("user", sys.argv[2]) \
        .option("password", sys.argv[3]) \
        .load()

    # employees_df.show(20)

    titles_df = spark.read.format("jdbc").option("driver", mysql_driver) \
        .option("url", mysql_url) \
        .option("dbtable", "titles") \
        .option("user", sys.argv[2]) \
        .option("password", sys.argv[3]) \
        .load()

    # titles_df.show(10)

    managers_df = spark.read.format("jdbc").option("driver", mysql_driver) \
        .option("url", mysql_url) \
        .option("dbtable", "dept_manager") \
        .option("user", sys.argv[2]) \
        .option("password", sys.argv[3]) \
        .load()

    # managers_df.show(10)

    employees_with_titles_df = employees_df.join(titles_df.where("to_date > '2024-01-01'"), ["emp_no"], how="left") \
        .select("emp_no", "first_name", "last_name", "title", "gender", "birth_date", "hire_date", "from_date")

    employees_with_titles_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("data/export/employees_with_titles.parquet")

    '''
    employees_with_titles_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("data/export/employees_with_titles.csv")

    employees_with_titles_df.write \
        .format("avro") \
        .mode("overwrite") \
        .save("data/export/employees_with_titles.avro")

    employees_with_titles_df.write \
        .format("json") \
        .mode("overwrite") \
        .save("data/export/employees_with_titles.json")
'''

    spark.stop()
    logger.info("Spark session stopped")

import sys
from lib.logger import Log4j
from lib.utils import get_spark_session

if __name__ == "__main__":
    job_run_env = sys.argv[1].upper()
    spark = get_spark_session(job_run_env)

    logger = Log4j(spark)
    logger.info("Spark session created")

    spark.stop()
    logger.info("Spark session stopped")

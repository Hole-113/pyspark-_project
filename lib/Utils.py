import logging
import logging.config
logging.config.fileConfig(fname='/home/hadoop/pyspark_project/configs/logging_to_file.conf')
logger=logging.getLogger('Utils')


from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config
def get_spark_session(env):
       try:
       
              logger.info(f"Environment variable is {env}")
              if env == "LOCAL":
                     master='local'
                     spark= SparkSession.builder \
                   .config(conf=get_pyspark_config(env)) \
                   .master(master) \
                   .getOrCreate()
              else:
                    master='yarn'
                    spark= SparkSession.builder \
                   .config(conf=get_pyspark_config(env)) \
                   .master(master) \
                   .enableHiveSupport() \
                   .getOrCreate()
       except Exception as exp:
              logging.error(str(exp),exc_info=True)
       else:
              return spark
              
import os
import logging
import logging.config
from pyspark.sql import SparkSession
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *

##Load the Logging Configuration File 
logging.config.fileConfig(fname='/home/hadoop/pyspark_project/configs/logging_to_file.conf')







def main():
    try:
    
        job_run_env = os.environ.get('ENVIRON')
        logging.info(job_run_env)
        logging.info("Creating Spark Session")
        spark = Utils.get_spark_session(job_run_env)
        spark.sparkContext.setLogLevel('ERROR')
        logging.info("Created Spark Session")
        orders_df = DataReader.read_orders(spark,job_run_env)
        orders_filtered = DataManipulation.filter_closed_orders(orders_df)
        customers_df = DataReader.read_customers(spark,job_run_env)
        joined_df = DataManipulation.join_orders_customers(orders_filtered,customers_df)
        aggregated_results = DataManipulation.count_orders_state(joined_df)
        aggregated_results.show()
        logging.info("end of main")
    except Exception as exp:
        logging.error(str(exp),exc_info=True)
    



if __name__ == '__main__':
    logging.info("Main method started......")
    main()
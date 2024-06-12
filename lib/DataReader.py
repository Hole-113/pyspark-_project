from lib import ConfigReader
import logging
import logging.config
logging.config.fileConfig(fname='/home/hadoop/pyspark_project/configs/logging_to_file.conf')
logger=logging.getLogger('DataReader')





#defining customers schema
def get_customers_schema():
        try:
            logger.info("get_customers_schema() function is started")
            schema = "customer_id int,customer_fname string,customer_lname string,username string,password string,address string,city string,state string,pincode string"
        except Exception as exp:
            logger.error(str(exp),exc_info=True)
        else:
            return schema   
    
    # creating customers dataframe
def read_customers(spark,env):
    try:
        logger.info("read_customers() function is started")   
        conf = ConfigReader.get_app_config(env)
        customers_file_path = conf["customers.file.path"]
    except Exception as exp:
        logger.error("Error in read_customers() method" + str(exp),exc_info=True)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_customers_schema()) \
            .load(customers_file_path)
    #defining orders schema
def get_orders_schema():
        schema = "order_id int,order_date string,customer_id int,order_status string"
        return schema
    #creating orders dataframe
def read_orders(spark,env):
        conf = ConfigReader.get_app_config(env)
        orders_file_path = conf["orders.file.path"]
        return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)
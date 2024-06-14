from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")
def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")
def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

def count_of_customers(spark,orders_df):
    orders_df.createOrReplaceTempView("orders")
    customers=spark.sql("select customer_id,count(customer_id) as total_orders from orders group By customer_id")
    return customers
    
    
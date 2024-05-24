from util import get_spark_session
from dotenv import load_dotenv
import os 

load_dotenv()


def main():
    env=os.environ.get("ENVIRON1")
    spark = get_spark_session(env,"spark_session_created")
    spark.sql("select curren_date").show() 
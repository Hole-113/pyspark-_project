from pyspark.sql import SparkSession


def get_spark_session(env, app_name):
    if env == 'Prod':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    return
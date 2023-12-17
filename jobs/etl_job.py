"""
etl_job.py
~~~~~~~~~~
"""
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit, udf

from dependencies.spark import start_spark
from pyspark.sql.types import StringType, ArrayType

spark, log, config = start_spark(app_name='my_etl_job', files=['configs/etl_config.json'])


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    print("Start the application.............")

    #spark, log, config = start_spark(app_name='my_etl_job', files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data.show(100)
    data_transformed = transform_data(data)
    data_transformed.show(100)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format."""
    df = (spark.read.parquet('D://DataEngineering_Projects//pyspark_etl_project//tests//test_data//employees'))
    return df

def get_values(result, config=config):
    return config[str(result)]

get_list_udf = udf(get_values,ArrayType(StringType()))

def transform_data(df):
    df_transformed = df.select(col('id'),concat_ws(' ',col('first_name'),col('second_name')).alias('name'))\
        .withColumn("result",get_list_udf(col("id"))).\
        withColumn("city",col("result")[1]).\
        withColumn("age",col("result")[0]).\
        withColumn("mobile",col("result")[2]).drop("result")
    return df_transformed

def load_data(df):
    """Collect data locally and write to CSV."""
    (df
     .coalesce(1)
     .write
     .csv('D:\\DataEngineering_Projects\\pyspark_etl_project\\hdfs_load_data', mode='overwrite', header=True))
    return None


def create_test_data(spark):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('D:\\DataEngineering_Projects\\pyspark_etl_project\\tests\\test_data\\employees\\', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df)
    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('D:\\DataEngineering_Projects\\pyspark_etl_project\\tests\\test_data\\employees_report\\', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

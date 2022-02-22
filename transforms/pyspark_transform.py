from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
import logging

spark = SparkSession.builder.master("local[*]") \
                    .appName('Python Spark RDD') \
                    .config("spark.sql.warehouse.dir","spark-warehouse" ) \
                    .enableHiveSupport() \
                    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("dfs.client.read.shortcircuit.skip.checksum", "true")
# spark.conf.set("parquet.enable.summary-metadata", "false")
sc = spark.sparkContext
sqlContext = SQLContext(sc)

# logger starts writing to app.log
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def read_csv(file: str, schema:str, header:bool=True,) -> object:
    """
    reads a specified file
    :param file: source file
    :param schema: data structure/schema
    :param header: if file contains header row
    :return: dataframe
    """
    file = f"data/{file}"
    try:
        data = spark.read.csv(file, header=False, schema=schema)
    except Exception as e:
        logging.error(f'read_csv failed: {e}')
        raise(e)
    return data

def data_cleanup(df):
    """
    Checks for data integrity and removes any invalid data identified
    :param df: dataframe from source
    :return: cleaned dataframe
    """
    logging.info(f"data_cleanup(): No of records before cleanup: {df.count()}")
    # 1. filter where booking has both exchanged and cancelled statuses
    df_cancel = df.filter("status='CANCELLED'").select('bookingId', 'status')
    # df_cancel.show(5)
    logging.info(f"data_cleanup(): No of cancelled bookings: {df_cancel.count()}")
    df_exchange = df.filter("status='EXCHANGED'").select('bookingId')
    logging.info(f"data_cleanup(): No of exchanged bookings: {df_exchange.count()}")
    df_exists = df_cancel.join(df_exchange, on=['bookingId'])
    if df_exists.count() > 0:
        logging.warning(f"data_cleanup(): No of exchanged and cancelled bookings: {df_exists.count()}")
        df = df.join(df_exists, on=['bookingId'], how='left_anti')
        write_file(df_exists,"err_cancel_vs_exch")
    # 2. booking date > departure date
    df2 = df.filter((datediff('bookingDate','departureDate')>0) & (df.status=='Booked'))
    if df2.count() > 0 :
        logging.warning(f"data_cleanup(): No of records with departure date before booking date : {df2.count()}")
        df_err =df.join(df2, on=['bookingId'], how='left_anti')
        write_file(df_err, "err_book_gt_depart")
    logging.info(f"data_cleanup(): No of records after cleanup: {df.count()}")
    return df

def write_file(df, name:str, type:str='csv') -> str:
    """
    Writes data from dataframe to file
    :param df: dataframe
    :param name: string denotes output folder
    :param type: string denotes file save as
    :return: filepath
    """
    out=f'data/{type}/{name}'
    logging.info(f"writing to file: {out}")
    if type == 'csv':
        if name == 'dates':
            df.write.partitionBy("bookingYear", "bookingMonth").mode('overwrite')\
                .option("header", True) \
                .csv(out)
        else:
            df.write.mode('overwrite') \
                .option("header", True) \
                .csv(out)
    elif type=='parquet':
        logging.info("write_file(): Saving as parquet")
        df.write.mode('overwrite').option("header", True).parquet(out)
    else:
        logging.warning("write_file(): No valid file type to save. Exitting.")

    return out

def read_parquet():
    """
    Reads from a parquet file
    :param df: dataframe
    :param name: string denotes output folder
    :param type: string denotes file save as
    :return: None
    """
    try:
        df = spark.read.parquet('data/parquet/parquet/*')
        return df.count()
    except Exception as e:
        logging.error(e)
        return 0

def get_booking_month_year(df):
    """
    Derive mon and year from booking date and add it to the dataframe - (Question 1)
    :param df: dataframe
    :return: None
    """
    dates_df = df.select(df.bookingId, df.travellerId, df.companyId,
                         df.bookingDate, df.departureDate, df.origin,
                         df.destination, df.priceInUSD, df.status,
                    date_format(df.bookingDate, 'MMM').alias('bookingMonth'),
                    date_format(df.bookingDate, 'yyyy').alias('bookingYear'))
    logging.info("Question1 - separate year, month and write to file...")
    # df_with_dates = partition_by_date(dates_df)
    write_file(dates_df, "dates")

def get_top_5_revenues(df):
    """
    Get companies with top 5 revenue (Question 2)
    :param df: dataframe
    :return: None
    """
    df_cancel = df.filter("status='CANCELLED'").select('bookingId', 'priceInUSD', 'companyId' )
    # working with sql
    df.createOrReplaceTempView("T1")
    df1 = spark.sql(
         "select companyId, sum(PriceInUSD) as total "
         "from T1 "
         "where status != 'CANCELLED' "
         "group by companyId "
         "order by total")
    spark.catalog.dropTempView("T1")
    # df1.show()
    df_cancel.createOrReplaceTempView("C1")
    df2 = spark.sql(
        "select companyId, sum(PriceInUSD) as cancellation "
        "from C1 "
        "group by companyId")
    spark.catalog.dropTempView("C1")
    # df2.show()
    df = df1.join(df2, on=['companyId'])\
            .select(df1.companyId, df1.total, df2.cancellation,
                    (df1.total-df2.cancellation).alias('revenue'))
    df=df.sort(df.revenue.desc())
    r_dict = [row.asDict() for row in df.collect()]
    logging.info(f"Companies with top 5 provisinal")
    write_file(df.limit(5).coalesce(1),"Question2")
    return df.count()

def get_top_5_destinations(df):
    """
        Get top 5 destinations with number of trips(Question 3)
        :param df: dataframe
        :return: None
    """
    # df.groupBy("department").sum("salary")
    df = df.select('destination', 'bookingId').distinct()
    df = df.groupBy("destination").agg(count("bookingId").alias('trips')).sort(desc('trips'))
    write_file(df.limit(5).coalesce(1), "Question3")
    # df.show(5)
    top_5_dest = df.limit(5).select('destination').rdd.flatMap(lambda x:x).collect()
    logging.info(f"Top 5 destinations: {top_5_dest}")
    return top_5_dest

def get_destination_list(df):
    write_file(df.select('destination').distinct().coalesce(1),"Question4")
    dest_lst = df.select('destination').distinct().rdd.flatMap(lambda x: x).collect()
    # print(dest_lst, f" No. of destinations: {len(dest_lst)}", sep="\n")
    # cleaned_df.select(collect_set("destination").alias('destination'), countDistinct('destination').alias('count')) #.show(truncate=False)
    logging.info(f"Destination List: {dest_lst}, No. of destinations: {len(dest_lst)}")
    return dest_lst

if __name__ == "__main__":
    # print(spark.version)
    schema =StructType([
    StructField("bookingId",StringType(), True),
    StructField("travellerId", IntegerType(), True),
    StructField("companyId", IntegerType(), True),
    StructField("bookingDate", DateType(), True),
    StructField("departureDate", DateType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("priceInUSD", IntegerType(), True),
    StructField("status", StringType(), True)])
    # read data from source
    df = read_csv('in/bookings.csv',header=False, schema=schema)
    # Extract month and year of booking date
    # Save as partitioned file
    get_booking_month_year(df)
    # cleanup & data integrity checks
    # 1. a booking cannot have both cancelled and exchange
    # 2. booking date cannot be greater than booking
    cleaned_df = data_cleanup(df)
    # filter top 5 companies with most revenue
    get_top_5_revenues(cleaned_df)
    # filter top 5 destinations and trips
    df_dest=get_top_5_destinations(cleaned_df)
    # get destination list and count
    get_destination_list(cleaned_df.select('destination'))
    # bonus question
    # Most travelled months (peak)
    df = cleaned_df.groupBy(date_format('departureDate', "MMM")\
               .alias('departureMonth'), date_format('departureDate', "yyyy").alias('departureYr'))\
        .agg(countDistinct("bookingId")
             .alias('trips'))\
        .sort(desc('departureYr'),desc('trips'))
    # df.show(5, truncate=False)
    write_file(df.limit(5).coalesce(1),"Bonus1")
    df_dict = [row.asDict() for row in df.collect()]
    logging.info(f"Most travelled months (peak): {df_dict}")

    # Least travelled months (non-peak)
    df = cleaned_df.groupBy(date_format('departureDate', "MMM") \
               .alias('departureMonth'), date_format('departureDate', "yyyy").alias('departureYr')) \
        .agg(countDistinct("bookingId")
             .alias('trips')) \
        .sort(desc('departureYr'), asc('trips')) # .show(5, truncate=False)
    write_file(df.limit(5).coalesce(1), "Bonus2")
    df_dict = [row.asDict() for row in df.collect()]
    logging.info(f"Least travelled months (peak): {df_dict}")

    write_file(cleaned_df,"parquet","parquet")
    # min, max, avg price by top destinations
    # cleaned_df.show(5)
    df2 = cleaned_df.filter(cleaned_df.destination.isin(df_dest)).filter("status='BOOKED'")\
                    .groupBy('origin','destination').agg(round(avg('priceInUSD'),2).alias('avgPrice'),
                                             min('priceInUSD').alias('minPrice'),
                                             max('priceInUSD').alias('maxPrice'))\
                    .sort('origin', 'destination')
    write_file(df2.coalesce(1), "Bonus3")
    # filter((cleaned_df.destination.isin(df_dest)))
    #
    # \
    # .groupBy('origin', 'destination', 'companyId') \
    #     .agg(min('priceInUSD').alias('minPrice'), \
    #          max('priceInUSD').alias('maxPrice'), round(avg('priceInUSD'), 2).alias('avgPrice')) \
    #     .sort('origin', 'destination')
    # df1.show(5)
    # df.filter("status = 'Booked'").show(5)



import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import logging.config
import psycopg2
import pandas
import pandas.io.sql as sqlio


class Ingest:
    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info('Ingesting from csv')
        try:
            df1 = self.spark.read.csv(
                "/user/saurabhdhoot95/cc_balance/credit_card_balance.csv", header=True)
            df2 = self.spark.read.csv(
                "/user/saurabhdhoot95/intstallment_payments/intstallments_payments.csv", header=True)
            df3 = self.spark.read.csv(
                "/user/saurabhdhoot95/pos_cash/pos_cash.csv", header=True)
            df4 = self.spark.read.csv(
                "/user/saurabhdhoot95/previous_application/previous_application.csv", header=True)
        except Exception as e:
            print("File not found ", e)
        #course_df = self.spark.sql("select * from futurex.retailcust")
        logger.info('DataFrame created')
        logger.warning('DataFrame created with warning')
        return df1, df2, df3, df4

    def read_from_pg(self):
        connection = psycopg2.connect(
            user='postgres', password='admin', host='localhost', database='postgres')
        cursor = connection.cursor()
        sql_query = "select * from futurexschema.futurex_course_catalog"
        pdDF = sqlio.read_sql_query(sql_query, connection)
        sparkDf = self.spark.createDataFrame(pdDF)
        sparkDf.show()

    def read_from_pg_using_jdbc_driver(self):

        jdbcDF = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
            .option("dbtable", "futurexschema.futurex_course_catalog") \
            .option("user", "postgres") \
            .option("password", "admin") \
            .load()

        jdbcDF.show()

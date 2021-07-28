import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import logging.config
import psycopg2
import pandas
import pandas.io.sql as sqlio
from pyspark.sql.functions import when
import pandas as pd


class Ingest:
    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info('Ingesting from csv')
        try:
            target1DF = self.spark.sql(
                "select * from bureau t1 inner join b_balance t2 on t1.sk_id_bureau = t2.sk_id_bureau where t1.credit_day_overdue=0 and t1.credit_active='Active' limit 10000")

            target2DF = target1DF.withColumn("new_status", when(target1DF.status == "C", "closed").when(target1DF.status == "X", "unknown").when(target1DF.status == "0", "No DPD").when(target1DF.status == "1", "DPD 1-30").when(
                target1DF.status == "2", "DPD 31-60").when(target1DF.status == "3", "DPD 61-90").when(target1DF.status == "4", "DPD 91-120").when(target1DF.status == "5", "DPD 121+").otherwise(target1DF.status))

            target3DF = target2DF.select('sk_id_curr', 't1.sk_id_bureau', 'credit_active', 'credit_currency', 'days_credit', 'credit_day_overdue', 'days_credit_enddate', 'days_enddate_fact', 'amt_credit_max_overdue',
                                         'cnt_credit_prolong', 'amt_credit_sum', 'amt_credit_sum_debt', 'amt_credit_sum_limit', 'amt_credit_sum_overdue', 'credit_type', 'days_credit_update', 'amt_annuity', 'month_balance', 'new_status')
            
            # create hive table 
            # Sqoop export 
            # target3DF.write.mode("overwrite").saveAsTable("default.balance_bureau");
            # target3DF.to_csv('balance_bureau.csv')

            target3DF.write.option("header","true") \
                .mode('overwrite') \
                .csv('/user/saurabhdhoo95/output/balance_bureau')


            # target3DF.write.format("csv").save("bank_prospects_transformed")

            # res = self.spark.sql(
            #     "select * from credit_card_balance t1 inner join intstallments_payments t2 on t1.sk_id_curr=t2.sk_id_curr inner join pos_cash_balance t3 on t3.sk_id_curr=t2.sk_id_curr where t1.name_contract_status <> 'Refused';")

            # res1 = res.select('t1.sk_id_prev','t1.sk_id_curr','t1.months_balance','amt_balance','amt_credit_limit_actual','amt_drawings_atm_current','amt_drawings_current','amt_drawings_other_current','amt_drawings_pos_current','amt_inst_min_regularity',
            # 'amt_payment_current','amt_payment_total_current','amt_receivable_principal','amt_recivable','amt_total_receivable','t1.name_contract_status','t1.sk_dpd','t1.sk_dpd_def','num_instalment_number','days_instalment','days_entry_payment','amt_instalment',
            # 'amt_payment','cnt_instalment','cnt_instalment_future')



        except Exception as e:
            print("File not found ", e)

        logger.info('DataFrame created')
        logger.warning('DataFrame created with warning')

        return target3DF

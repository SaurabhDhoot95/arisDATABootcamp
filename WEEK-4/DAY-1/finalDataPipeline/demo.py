import sys
from pyspark.sql import SparkSession
from pipeline import transform, persist, ingest
import logging
import logging.config
import subprocess



def createSparkSession():
    spark = SparkSession.builder\
            .appName("my first spark app")\
            .config("spark.driver.extraClassPath", "pipeline/postgresql-42.2.18.jar")\
            .enableHiveSupport().getOrCreate()
    return spark

def runPipeline(spark):
    try:
        # df = spark.sql("select * from test1")
        
        df=spark.read.format("csv").load("part-00000-32402b2e-c886-4e05-b66f-fc0a031044d9-c000.csv")
        # part-00000-32402b2e-c886-4e05-b66f-fc0a031044d9-c000.csv
        df.show() 
        df.createGlobalTempView('newDF')
        command = "sqoop export --connect jdbc:mysql://localhost:3306/export_demo --username root --password password -export-dir /user/hive/warehouse/newDF --table export_test2 --input-fields-terminated-by ','"
        subprocess.run(command)
    except Exception as e:
        print(e)
    
spark = createSparkSession()
runPipeline(spark)

# class Pipeline:
#     logging.config.fileConfig("pipeline/resources/configs/logging.conf")

#     def run_pipeline(self):
#         try:
#             logging.info('run_pipeline method started')
#             ingest_process = ingest.Ingest(self.spark)
#             df = ingest_process.ingest_data()
#             df.show(5)
#             #for i in range(len(df)):
#             #    df[i].show(5)

#         except Exception as exp:
#             logging.error(
#                 "An error occured while running the pipeline > " + str(exp))
#             sys.exit(1)

#         return

#     def create_spark_session(self):
#         self.spark = SparkSession.builder\
#             .appName("my first spark app")\
#             .config("spark.driver.extraClassPath", "pipeline/postgresql-42.2.18.jar")\
#             .enableHiveSupport().getOrCreate()


# if __name__ == '__main__':
#     logging.info('Application started')
#     pipeline = Pipeline()
#     pipeline.create_spark_session()
#     logging.info('Spark Session created')
#     pipeline.run_pipeline()
#     logging.info('Pipeline executed')



from subprocess import run,PIPE
import re
try:
    command='''sqoop eval \
    --connect jdbc:mysql://localhost:3306/demo \
    --username root \
    --password password \
    --query "select count(*) from new_tbl_application_train"'''    

    # Its working
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True,shell=True)
    outString=result.stdout
    if result.returncode == 0:
        mysqlCount = 0
        hiveCount = 0
        if 'count(*)' in outString:
            count = re.findall(r'\d+', outString)[0]
            mysqlCount =int(count)  

        cmd2 = 'hdfs dfs -cat newAppTrain_02/part-m-* | wc -l'  
        result2 = run(cmd2, stdout=PIPE, stderr=PIPE, universal_newlines=True,shell=True)
        hiveCount=int( re.findall(r'\d+',result2.stdout)[0])
        if mysqlCount == hiveCount :
             print('Import part is successfully complted!!!!')       

    # print(a)
except Exception as e:
    print(e)



import json
import subprocess


with open('config.json') as f:
    data = json.load(f)



try:
    # command = '''sqoop import --connect {connectionstr} --username {id} --password {passwrd} --table {tname} \
    # --target-dir {trgtDir} --hive-import --create-hive-table --hive-table {dbname}'''.format(connectionstr=data['conn'],
    #                                                                                         id=data['username'],
    #                                                                                         passwrd=data['password'],
    #                                                                                         tname=data['tname'],
    #                                                                                         trgtDir=data['targetdir'],
    #                                                                                         dbname=data['targethive']
    #                                                                                         )


    cmd = '''sqoop import \
--connect {connectionstr} \
--username {id} \
--password {password} \
--table {tname} \
--target-dir {trgtDir} \
--hive-import \
--create-hive-table \
--hive-table {dbName}'''.format(connectionstr=data['conn'],
 id=data['username'], 
 password=data['password'],
 tname=data['tname'],
 trgtDir=data['targetdir'],
 dbname=data['targethive'])
                                                                                            

    subprocess.run(command, shell=True)
    print(command)
except Exception as e:
    print(e)


print("Success !!!!!!!!!")














import json
import subprocess


with open("config.json") as f:
    data = json.load(f)


try:
    cmd="""sqoop import \
--connect {connectionstr} \
--username {id} \
--password {passwrd} \
--table {tname} \
--target-dir {trgtDir} \
--hive-import \
--create-hive-table \
--hive-table {dbname}""".format(
        connectionstr=data["connURL"],
        id=data["userName"],
        passwrd=data["passWord"],
        tname=data["mysqlTblName"],
        trgtDir=data["targetDir"],
        dbname=data["hiveTblName"],
    )

    subprocess.run(cmd, shell=True)
    print(cmd)
except Exception as e:
    print(e)


{
"connURL": "jdbc:mysql://localhost:3306/demo",
"userName": "root",
"passWord": "password",
"mysqlTblName": "new_tbl_application_train",
"targetDir": "output/appTrain",
"hiveTblName": "sqoop_workspace.appTrain"
}


print("Success !!!!!!!!!")



"""sqoop import \
--connect jdbc:mysql://localhost:3306/demo \
--username root \
--password password \
--table new_tbl_application_train \
--target-dir newAppTrain_02 \
--hive-import \
--create-hive-table \
--hive-table sqoop_workspace.app_train02"""












import json
import subprocess


with open("config.json") as f:
    data = json.load(f)


try:                                                                        
    cmd = """sqoop import \
--connect {connectionstr} \
--username {id} \
--password {password} \
--table {tname} \
--target-dir {trgtDir} \
--hive-import \
--create-hive-table \
--hive-table {dbName}""".format(
        connectionstr=data["conn"],
        id=data["username"],
        password=data["password"],
        tname=data["tname"],
        trgtDir=data["targetdir"],
        dbname=data["targethive"],
    )

    subprocess.run(command, shell=True)
    print(command)
except Exception as e:
    print(e)


import json
import subprocess


with open("config.json") as f:
    data = json.load(f)


try:
    cmd = '''sqoop import \
--connect {connectionstr} \
--username {id} \
--password {password} \
--table {tname} \
--target-dir {trgtDir} \
--hive-import \
--create-hive-table \
--hive-table {dbName}'''.format(
        connectionstr=data["conn"],
        id=data["username"],
        password=data["password"],
        tname=data["tname"],
        trgtDir=data["targetdir"],
        dbname=data["targethive"],
    )

    subprocess.run(command, shell=True)
    print(command)
except Exception as e:
    print(e)

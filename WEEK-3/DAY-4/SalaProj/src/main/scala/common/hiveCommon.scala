package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory


object hiveCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def createHiveTable(spark: SparkSession): Unit = {
    logger.warn("createHiveTable() method started")
//    spark.sql("create database if not exists demo")
//    spark.sql("create table if not exists demo.employee(emp_id string,department string, emp_name string,dept_id string)")
//    spark.sql("insert into demo.employee VALUES (1,'Sales','Saurabh',100)")
//    spark.sql("insert into demo.employee VALUES (2,'Sales','Sarvesh',100)")
//    spark.sql("insert into demo.employee VALUES (3,'Development','Ajinkya',101)")
//    spark.sql("insert into demo.employee VALUES (4,'HR','Harshu',104)")
//    spark.sql("insert into demo.employee VALUES (5,'Accounts','Anjali',103)")
//    spark.sql("insert into demo.employee VALUES (6,'CS','',100)")
//    spark.sql("insert into demo.employee VALUES (7,'CS','Sam','')")
//    spark.sql("insert into demo.employee VALUES (8,'Development','Gani',101)")
//    spark.sql("insert into demo.employee VALUES (9,'Development','Vijay',101)")
//    spark.sql("insert into demo.employee VALUES (10,'Accounts','Shital',103)")
//    spark.sql("insert into demo.employee VALUES (11,'Jenkins','Ashish',102)")
//    spark.sql("insert into demo.employee VALUES (12,'HR','Piyush',104)")
    spark.sql("insert into demo.employee VALUES (14,'Accounts','abc',102)")

    // Treat empty strings as Null
    spark.sql("alter table demo.employee set tblproperties('serialization.null.format'='')")

  }
  def readHiveTable(spark : SparkSession) : Option[DataFrame] = {
    try {
      logger.warn("readHiveTable() method started")
      val courseDF = spark.sql("select * from demo.employee")
      logger.warn("readHiveTable() method ended")
      Some(courseDF)
    } catch {
      case e: Exception =>
        logger.error("Error Reading demo.employee "+e.printStackTrace())
        None
    }
  }
  def filterDataframe(df: DataFrame) {
    df.filter(df("emp_id") < '5').show()
//    Some(df)

  }
}

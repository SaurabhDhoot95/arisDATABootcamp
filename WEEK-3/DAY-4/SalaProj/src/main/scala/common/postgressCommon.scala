package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object postgressCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def getPostgressCommonProps(): Properties = {
    logger.info("getPostgressCommonProps() Started")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user", "postgres")
    pgConnectionProperties.put("password", "431107")
    pgConnectionProperties
  }

  def getPostgressServerDatabase() : String = {
    logger.info("getPostgressServerDatabase() Started")
    val pgURL = "jdbc:postgresql://localhost:5432/futurex"
//    val pgURL = "jdbc:postgresql://localhost:5432/demo"
    pgURL
  }

  def fetchDataFrameFrompgTable(spark:SparkSession) :Option[DataFrame] = {
    try{
      logger.info("fetchDataFrameFrompgTable() Started")
      val pgProp = getPostgressCommonProps()
      val pgURLDetails = getPostgressServerDatabase()
      logger.info("Creating Dataframe from Postgres")
      val pgTable =  "futureschema.futurex_course_catalog"
      val pgCourseDataframe = spark.read.jdbc(pgURLDetails,pgTable,pgProp)
      Some(pgCourseDataframe)
    } catch {
      case e: Exception =>
        logger.error("An error has occured in fetchDataFrameFromPgTable "+ e.printStackTrace())
        System.exit(1)
        None
    }
  }

  def writeDataToPgTable(dataFrame: DataFrame, pgTable : String): Unit ={
    try {
      logger.warn("writeDFToPostgresTable method started")

      dataFrame.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url","jdbc:postgresql://localhost:5432/demo" )
        .option("dbtable", pgTable)
        .option("user", "postgres")
        .option("password", "431107")
        .save()
      logger.warn("writeDFToPostgresTable method ended")

    } catch {
      case e: Exception =>
        logger.error("An error occured in writeDFToPostgresTable "+ e.printStackTrace())
    }

  }
}

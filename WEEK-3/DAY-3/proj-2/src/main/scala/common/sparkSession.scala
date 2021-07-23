package common

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.slf4j.LoggerFactory
import java.util.Properties

object sparkSession {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): Option[SparkSession] = {
    try {
      logger.info("createSparkSession() started")

      System.setProperty("hadoop.home.dir", "C:\\Windows\\winutils")
      val spark = SparkSession
        .builder()
        .appName(name = "HelloSpark")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()

      println("Created Spark Session")

      val sampleReq = Seq((1, "spark"), (2, "Big Data"))
      val df = spark.createDataFrame(sampleReq).toDF(colNames = "course id", "course name")
      df.show()
      Some(spark)

    } catch {
      case e: Exception =>
        logger.error("An error has occured in Spark session creation method " + e.printStackTrace())
        System.exit(1)
        None
    }
    //    def main(args: Array[String]): Unit = {
    //      println("Hello spark scala")
    //
    //      // Create spark session
    //      System.setProperty("hadoop.home.dir","C:\\Windows\\winutils")
    //      val spark = SparkSession
    //        .builder()
    //        .appName(name = "HelloSpark")
    //        .config("spark.master","local")
    //        .enableHiveSupport()
    //        .getOrCreate()
    //
    //      val sampleReq = Seq((1,"spark"),(2,"Big Data"))
    //      val df = spark.createDataFrame(sampleReq).toDF(colNames = "course id","course name")
    //      df.show()
    //      //df.write.format(source = "csv").save(path = "sampleReq")
    //
    //      println("Creating Dataframe from PostgreSql Database")
    //      val pgConnectionProperties = new Properties()
    //      pgConnectionProperties.put("user","postgres")
    //      pgConnectionProperties.put("password","431107")
    //
    //      val pgTable = "futureschema.futurex_course_catalog"
    //      val pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://localhost:5432/futurex",pgTable,pgConnectionProperties)
    //
    //      pgCourseDataframe.show()
    //      println("Shown")
    //    }

  }
}

import common.sparkSession
import common.postgressCommon
import org.slf4j.LoggerFactory

object SparkTransformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    try {
      logger.info("Main method started")
      logger.warn("This is a warning")
      val spark = sparkSession.createSparkSession().get
      val fetchedDataframe = postgressCommon.fetchDataFrameFrompgTable(spark).get
      fetchedDataframe.show()
      logger.info("Successfully ended !!!")
    }
  }
}




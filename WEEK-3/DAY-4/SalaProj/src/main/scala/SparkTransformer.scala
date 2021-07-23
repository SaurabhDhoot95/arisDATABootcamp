import common.{hiveCommon, postgressCommon, sparkSession}
import org.slf4j.LoggerFactory


object SparkTransformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    try {
      logger.info("Main method started")
      logger.warn("This is a warning")
      val spark = sparkSession.createSparkSession().get
//      val fetchedDataframe = postgressCommon.fetchDataFrameFrompgTable(spark).get
//      fetchedDataframe.show()
//      hiveCommon.createHiveTable(spark)
      val dataFrame  = hiveCommon.readHiveTable(spark).get
      dataFrame.show()
      val newDataFrame = hiveCommon.filterDataframe(dataFrame)b
//      val pgTable = "demo.employee"
//      postgressCommon.writeDataToPgTable(dataFrame,pgTable)
      logger.info("Successfully ended !!!")
    }
  }
}

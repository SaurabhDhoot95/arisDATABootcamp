import org.apache.spark.sql.SparkSession
import java.util.Properties

object SparkTransformer {
  def main(args: Array[String]): Unit = {
    println("Hello spark scala")

    // Create spark session
    System.setProperty("hadoop.home.dir","C:\\Windows\\winutils")
    val spark = SparkSession
      .builder()
      .appName(name = "HelloSpark")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    val sampleReq = Seq((1,"spark"),(2,"Big Data"))
    val df = spark.createDataFrame(sampleReq).toDF(colNames = "course id","course name")
    df.show()
    //df.write.format(source = "csv").save(path = "sampleReq")

    println("Creating Dataframe from PostgreSql Database")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","431107")

    val pgTable = "futureschema.futurex_course_catalog"
    val pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://localhost:5432/futurex",pgTable,pgConnectionProperties)

    pgCourseDataframe.show()
    println("Shown")
  }
}

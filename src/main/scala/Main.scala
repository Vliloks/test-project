import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}


object Main extends App {

  val applicationConf: Config = ConfigFactory.load("application.conf")

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


//    Путь к файлам
//    val dirName = "src/main/resources/generated"
  val dirName = applicationConf.getString("env.dirName")

//    Ожидаемый размер файла
//    val givenSize= args(0)
  val givenSize = applicationConf.getString("env.givenSize")
  val defaultSize = 128

  var reqSize = try {
    givenSize.toInt
  } catch {
    case e: Exception => defaultSize
  }

  reqSize = if (reqSize == 0) defaultSize else reqSize


  val smallFiles = new SmallFiles(spark)
  smallFiles.generateSmallFiles(dirName)

  val compactClass = new CompactClass()
  compactClass.compact(reqSize, dirName, spark)
//  compactClass.saveMetaInfoInDB(dirName, spark)

  spark.stop()

}

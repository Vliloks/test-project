import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}


object Main {
  def main(args: Array[String]): Unit = {

    val applicationConf: Config = ConfigFactory.load("application.conf")
    val appname = applicationConf.getString("app.owner")
    print(appname)

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    Путь к файлам
//    val dirName: String = args(1)
    val dirName = "src/main/resources/generated"

//    Ожидаемый размер файла
//    val reqSize= args(0)
//    val reqSize = "10"

    val defaultSize = 128

    var reqSize = if (args.length == 0) defaultSize else try {
      args(0).toInt
    } catch {
      case e: Exception => defaultSize
    }

    reqSize = if (reqSize == 0) defaultSize else reqSize


    val smallFiles = new SmallFiles(spark)
    smallFiles.generateSmallFiles(dirName)

    val compactClass = new CompactClass()
    compactClass.compact(reqSize, dirName, spark)
    compactClass.saveMetaInfoInDB(dirName, spark)


  }
}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io.File


object Main {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkApp")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    Путь к файлам
    val smallFilesPath: String = args(0)
//    val smallFilesPath: String = "src/main/resources/generated"

    deleteAllFilesFolder(smallFilesPath)

    val smallFiles = new SmallFiles(spark)
    smallFiles.generateSmallFiles(smallFilesPath)

//    Ожидаемый размер файла
    val expectedSize: String = args(1)
//    val expectedSize: String = "10"

    val compactDir: String = s"$smallFilesPath/compact"
    val compactClass = new CompactClass(compactDir)
    compactClass.compact(expectedSize, smallFilesPath, spark)
    compactClass.saveMetaInfoInDB(smallFilesPath, spark)


  }

  def deleteAllFilesFolder(smallFilesPath: String): Unit = {

    val folder = new File(smallFilesPath)
    val fileList = folder.listFiles()
    scanFileList(folder, fileList)
  }

  def scanFileList(folder: File, fileList: Array[File]): Unit = {
    if (fileList != null){
      for (file <- fileList){
        if(file.isFile){
          file.delete()
        } else scanFileList(file, file.listFiles())
      }
    }
  }
}

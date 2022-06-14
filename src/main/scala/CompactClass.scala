import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class CompactClass() {
  val hdfs = FileSystem.get(new Configuration())

  def compact(reqSize: Int, dirName: String, spark: SparkSession): Unit ={


    val iter = hdfs.listFiles(new Path(dirName), false)
    var dirSize = 0L
    while (iter.hasNext) dirSize = dirSize + iter.next().getLen
    println("#dirSize: "+dirSize)
    println("#reqSize: "+reqSize)
    val repartitionFactor = math.ceil(dirSize.toDouble/1000000/reqSize).toInt
    println("#repartitionFactor: "+repartitionFactor)


    val pathTmp = s"$dirName/../tmp"

    val df = spark.read.option("mergeSchema", "true").parquet(dirName)

    df
      .repartition(repartitionFactor)
      .write
      .mode(SaveMode.Append)
      .parquet(pathTmp)

    hdfs.delete(new Path(dirName), true)
    hdfs.rename(new Path(pathTmp), new Path(dirName))
  }

  def saveMetaInfoInDB(dirName: String ,spark: SparkSession): Unit ={
    val iter = hdfs.listFiles(new Path(dirName), false)
    var numberOfFiles = 0
    var overallSize = 0L

    while (iter.hasNext) {
      var f = iter.next()
      if (f.getLen != 0) {
        numberOfFiles = numberOfFiles + 1
        overallSize = overallSize + f.getLen
      }
    }

    val averageFilesSize = if (numberOfFiles > 0) overallSize.toDouble / numberOfFiles / 1000000 else 0

    val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now())


    val info_Seq = Seq(Row(dirName, numberOfFiles, averageFilesSize, timestamp))

    val info_schema = StructType( Array(
      StructField("data_path", StringType),
      StructField("number_of_files", IntegerType),
      StructField("average_files_size", DoubleType),
      StructField("dt", StringType)
    ))

    val info_df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(info_Seq), info_schema)

    val conf = ConfigFactory.load("application.conf")
    val url = conf.getString("postgres.url")
    val dbtable = conf.getString("postgres.dbtable")
    val user = conf.getString("postgres.user")
    val password = conf.getString("postgres.password")

    info_df.select("data_path", "number_of_files", "average_files_size", "dt").write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", url)
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", dbtable)
          .option("user", user)
          .option("password", password)
          .save()

  }


}

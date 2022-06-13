import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class CompactClass(compactDir:String) {

  def compact(expectedSize: String, smallFilesPath: String, spark: SparkSession): Unit ={

    val dirSize = FileUtils.sizeOfDirectory(new File(smallFilesPath)).toDouble/1000000
    val repartition_factor = math.ceil(dirSize/expectedSize.toDouble).toInt

    val df = spark.read.parquet(smallFilesPath)
    df
      .repartition(repartition_factor)
//      .coalesce(repartition_factor)
      .write
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .mode(SaveMode.Overwrite)
      .parquet(compactDir)

  }

  def saveMetaInfoInDB(dirname: String ,spark: SparkSession): Unit ={

    val numberOfFiles = new File(compactDir).listFiles().length
    val average_files_size = FileUtils.sizeOfDirectory(new File(compactDir)).toDouble/numberOfFiles/1000000
    val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now())


    val info_Seq = Seq(Row(compactDir, numberOfFiles, average_files_size, timestamp))

    val info_schema = StructType( Array(
      StructField("data_path", StringType,true),
      StructField("number_of_files", IntegerType, true),
      StructField("average_files_size", DoubleType, true),
      StructField("dt", StringType,true)
    ))

    val info_df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(info_Seq), info_schema)

    info_df.select("data_path", "number_of_files", "average_files_size", "dt").write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", "jdbc:postgresql://127.0.0.1:5432/test_task_db")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "meta_info_table")
          .option("user", "postgres")
          .option("password", "5036")
          .save()

//    postgresql_df.sqlContext.sql("""CREATE TABLE IF NOT EXISTS meta_info_table (
//      ID SERIAL PRIMARY KEY,
//      data_path VARCHAR(225),
//      number_of_files INTEGER,
//      average_files_size DOUBLE PRECISION,
//      dt VARCHAR
//      );""")


  }


//  def compact2(expectedSize: String, dirname: String, spark: SparkSession): Unit ={
//    val dirSize = FileUtils.sizeOfDirectory(new File(dirname)).toDouble/1000000
//    val repartition_factor = math.ceil(dirSize/expectedSize.toDouble).toInt
//    System.out.println("#repartition_factor: " + repartition_factor)
//
//    val df = spark.read.parquet(dirname)
//
//    df.withColumn("input_file_name_part", regexp_extract(input_file_name(), """part.+c\d{3}""", 0))
//      .select("input_file_name_part")
//      .distinct
//      .write
//      .parquet(s"$dirname/input_file_name_parts")
//
//    val fileNames = spark.read
//      .parquet(s"$dirname/input_file_name_parts")
//      .collect
//      .map((r: Row) => r(0).asInstanceOf[String])
//
//    val uncompactedDF = spark.read
//      .parquet(s"$dirname/{${fileNames.mkString(",")}}*.parquet")
//
//    uncompactedDF
//      .coalesce(repartition_factor)
//      .write
//      .mode(SaveMode.Append)
//      .parquet(dirname)
//
//
//    fileNames.foreach { filename: String =>
//      new File(s"$dirname/$filename.snappy.parquet").delete()
//    }
//
//    deleteAllFilesFolder(s"$dirname/input_file_name_parts")
//
//
//  }
}

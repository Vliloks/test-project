import scala.util.Random
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class SmallFiles(spark: SparkSession) {
  def generateDf(numberOfCols: Int, numberOfRows: Int): DataFrame = {
    val schema = StructType(
      Array.fill(numberOfCols)(
        StructField(
          Random.alphanumeric.dropWhile(_.isDigit).take(10).mkString,
          DoubleType
        )
      )
    )

    val rdd = spark.sparkContext
      .parallelize(
        (0 to numberOfRows)
          .map(_ => Row.fromSeq(Seq.fill(numberOfCols)(Random.nextDouble)))
      )

    spark.createDataFrame(
      rdd,
      schema
    )
  }

  def generateSmallFiles(smallFilesPath: String): Unit = {
    generateDf(10, 4000000)
      .repartition(30)
      .write
      .mode(SaveMode.Append)
      .parquet(smallFilesPath)
  }
}

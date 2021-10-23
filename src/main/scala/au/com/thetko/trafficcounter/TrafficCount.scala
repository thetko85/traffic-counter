package au.com.thetko.trafficcounter

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.Window
import java.sql.{Timestamp,Date}
import scala.util.Try
import org.apache.spark.sql.SparkSession
import scala.util.Success
import java.io.FileNotFoundException
import scala.util.Failure

case class TrafficCount(timestamp: Timestamp, count: Int)
case class TrafficDailyCount(date: Date, total: Long)
case class TrafficWindowCount(window_name: String, total: Long)

object TrafficCount {
  val Schema = StructType(Seq(StructField("timestamp", DataTypes.TimestampType), StructField("count", DataTypes.IntegerType)))

  def createDataSet(spark: SparkSession, csvFile: String): Try[Dataset[TrafficCount]] = {
    import spark.implicits._
    try {
      val ds = spark.read
      .schema(TrafficCount.Schema)
      .csv(csvFile)
      .as[TrafficCount]

      Success(ds)
    } catch {
      case ex: FileNotFoundException => {
        println(s"File $csvFile not found")
        Failure(ex)
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        Failure(unknown)
      }
    }
  }
  
  def getTotalCars(ds: Dataset[TrafficCount]): DataFrame = {
    ds.agg(functions.sum("count").cast("long").as("total"))
  }

  def getDailyCars(ds: Dataset[TrafficCount]): Dataset[TrafficDailyCount] = {
    import ds.sparkSession.implicits._
    ds.withColumn("date", $"timestamp".cast("date"))
    .groupBy("date")
    .agg(functions.sum("count").cast("long").alias("total"))
    .orderBy($"date".asc)
    .as[TrafficDailyCount]
  }

  def getCarsOverThreeContiguousHalfHours(ds: Dataset[TrafficCount]): Dataset[TrafficWindowCount] = {
    val window = Window.orderBy(functions.asc("timestamp"))
    import ds.sparkSession.implicits._

    ds.withColumn("seconds", $"timestamp".cast("long"))
    .withColumn("window_name", functions.concat(functions.lag("timestamp", 1).over(window) - functions.expr("INTERVAL 30 MINUTES"), functions.lit(" to "), functions.lead("timestamp", 1).over(window)))
    .withColumn("neighbouring_window_duration_seconds", functions.lead("seconds", 1).over(window) - $"seconds" + $"seconds" - functions.lag("seconds", 1).over(window))
    .withColumn("total", functions.lead("count", 1).over(window) + $"count" + functions.lag("count", 1).over(window))
    .filter("neighbouring_window_duration_seconds=3600")
    .select("window_name","total")
    .as[TrafficWindowCount]
  }
}
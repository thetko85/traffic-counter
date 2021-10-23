package au.com.thetko.trafficcounter

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import au.com.thetko.trafficcounter.Config
import au.com.thetko.trafficcounter.TrafficCount
import scala.util.Success
import scala.util.Failure
import org.apache.spark.sql.SaveMode
import java.nio.file.Paths
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object Main extends App  {
  val conf = new Config(args)

  val spark = SparkSession.builder.getOrCreate
  import spark.implicits._
  
  val log = LogManager.getLogger(Main.getClass)  
  log.setLevel(Level.INFO)

  // Hide the root info/warn logs as we will be displaying our output to console
  spark.sparkContext.setLogLevel("ERROR")

  TrafficCount.createDataSet(spark, conf.input.apply) match {
    case Success(ds) => {
      // Question 1
      // The number of cars seen in total
      log.info("Question 1: Total Cars")
      TrafficCount.getTotalCars(ds).show(false)

      // Question 2
      // Daily Car Count
      log.info("Question 2: Daily Car Count")
      TrafficCount.getDailyCars(ds).show(false)

      // Question 3
      // Top three half hours with most cars
      log.info("Question 3: Top three half hours with most cars")
      ds.orderBy($"count".desc).limit(3).show(false)

      // Question 4
      // The 1.5 hour period with least cars
      log.info("Question 4: 1.5 hour window with least cars")
      TrafficCount.getCarsOverThreeContiguousHalfHours(ds).orderBy($"total".asc).limit(1).show(false)
    }
    case Failure(ex) => {
      throw ex
    }
  }

  spark.stop()

}
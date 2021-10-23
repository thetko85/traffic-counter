package au.com.thetko.trafficcounter

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLContext
import au.com.thetko.trafficcounter.{TrafficCount}
import java.sql.Date
import scala.util.Success
import scala.util.Failure

class TrafficCountTest extends FunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
  import testImplicits._
  override def beforeEach(): Unit = {
    spark = SparkSession.builder().master("local[*]").getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  def getTestCSVFilePath(): String = {
    val file = new File("src/test/resources/traffic_count.csv");
    file.getAbsolutePath()
  }

  test("Total cars should equal 267") {
    TrafficCount.createDataSet(spark, getTestCSVFilePath) match {
      case Success(ds) => {
        assert(TrafficCount.getTotalCars(ds).first.get(0) == 267L)
      }
      case Failure(exception) => {
        throw exception
      } 
    }
  }

  test("Validate Daily Car Count") {
    TrafficCount.createDataSet(spark, getTestCSVFilePath) match {
      case Success(ds) => {
        val dailyCount = TrafficCount.getDailyCars(ds)
        assert(dailyCount.take(3).sameElements(Array(TrafficDailyCount(Date.valueOf("2016-12-01"), 230L), TrafficDailyCount(Date.valueOf("2016-12-02"), 36L), TrafficDailyCount(Date.valueOf("2016-12-03"), 1L))))
      }
      case Failure(exception) => {
        throw exception
      } 
    }
  }

  test("Least Cars over 1.5 hours should be 2016-12-01 09:00:00 to 2016-12-01 10:30:00") {
    TrafficCount.createDataSet(spark, getTestCSVFilePath) match {
      case Success(ds) => {
        val dsWindowed = TrafficCount.getCarsOverThreeContiguousHalfHours(ds)

        assert(dsWindowed.count() == 21L, "there are 21 contiguous windows")
        assert(dsWindowed.orderBy($"total".asc).first() == TrafficWindowCount("2016-12-01 09:00:00 to 2016-12-01 10:30:00", 2L), "test smallest time window")
      }
      case Failure(exception) => {
        throw exception
      } 
    }
  }
}
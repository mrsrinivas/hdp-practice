import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Created by Srinivas Reddy Alluri on 27-09-2016.
  */
object CsvReader {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "..\\..\\spark-wh")
    System.setProperty("hadoop.home.dir", "..\\..\\")

    Logger.getLogger(CsvReader.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate;

    //implicits for magic functions

    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").csv("input/csv/input1.csv")

    df.show()


  }
}

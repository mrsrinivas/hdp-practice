package main.java

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Created by Srinivas Reddy Alluri on 27-09-2016.
  */
object JsonWritter {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "..\\..\\spark-wh")
    System.setProperty("hadoop.home.dir", "..\\..\\")


    Logger.getLogger(JsonWritter.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate;

    //implicits for magic functions like .toDf
    import spark.implicits._

    val df = Seq(
      (2012, 8, "Batman", 9.8),
      (2012, 8, "Hero", 8.7),
      (2012, 7, "Robot", 5.5),
      (2011, 7, "Git", 2.0)
    ).toDF("year", "month", "title", "rating").repartition(2);

    df.write.json("output/movies.json")

    //df.repartition(1).write.json("output/movies.json")


  }
}

package main.java

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Created by Srinivas Reddy Alluri on 27-09-2016.
  */
object AvroReader {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "..\\..\\spark-wh")
    System.setProperty("hadoop.home.dir", "..\\..\\")

    Logger.getLogger(AvroReader.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

    //for magic functions like .toDf on rdd and $ for column names
   // import spark.implicits._

    val df = spark.read.format("json").json("output/jsn/*/*.json")

    df.show()



    //df.repartition(1).write.json("output/movies.json")


  }
}

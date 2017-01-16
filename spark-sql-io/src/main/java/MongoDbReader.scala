package main.java

import com.mongodb.spark.config.ReadConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Created by Srinivas Reddy Alluri on 27-09-2016.
  */
object MongoDbReader {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "..\\..\\spark-wh")
    System.setProperty("hadoop.home.dir", "..\\..\\")

    Logger.getLogger(MongoDbReader.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //System.setProperty("mapred.output.compress", "false")


    val spark: SparkSession = SparkSession.builder.master("local").appName(MongoDbReader.toString).getOrCreate;

    //implicits for magic functions
    import spark.implicits._

    val readConfig: ReadConfig = ReadConfig(Map(
      "spark.mongodb.input.uri"-> "mongodb://mongodb01.blabla.com/xqwer",
      "collection" -> "some_collection"),
      None)

    import org.apache.spark.sql.functions._
    val df = spark.read.format("com.mongodb.spark.sql").options(readConfig.asOptions).load()
    df.filter($"uid".equalTo(lit("ZesSZY3Ch0k8nQtQUIfH"))).explain(true);



  }
}

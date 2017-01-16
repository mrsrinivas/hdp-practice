package main.java

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction


/**
  * Created by Srinivas Reddy Alluri on 27-09-2016.
  */
object XmlReader {

  def get_only_file_name(fullPath: String): String = {
    fullPath.split("/").last
  }


  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "..\\..\\spark-wh")
    System.setProperty("hadoop.home.dir", "..\\..\\")


    Logger.getLogger(XmlReader.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate;

    //implicits for magic functions

    val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "NewTag").load("input/xml/rows.xml")

    val xmldf = spark.sql("select _xmlns, `md:Date`, `md:Creator`, struct(_ngr, _region, SetofValues) as Station from (select _xmlns, `md:Date`, `md:Creator`, _ngr, _region, struct(_dataType, _period, Value) as SetofValues  from (select _xmlns, `md:Date`, `md:Creator`, _ngr, _region, _dataType, _period, struct(_VALUE, _time) as Value from df_h a left outer join df_ds b on a.batchId = b.batchId left outer join df_dsv c on b.batchId = c.batchId left outer join df_nv d on c.batchId = d.batchId))");

    xmldf.printSchema()


  }
}

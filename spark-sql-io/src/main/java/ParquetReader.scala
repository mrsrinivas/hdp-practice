package com.practice.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Created by Srinivas Reddy Alluri on 27-09-2016.
  */
object ParquetReader {

  def main(args: Array[String]) {

    System.setProperty("spark.sql.warehouse.dir", "..\\..\\spark-wh")
    System.setProperty("hadoop.home.dir", "..\\..\\")

    Logger.getLogger(ParquetReader.toString).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //System.setProperty("mapred.output.compress", "false")


    val spark: SparkSession = SparkSession.builder.master("local").appName(ParquetReader.toString).getOrCreate;

    //implicits for magic functions
    import spark.implicits._

    //    val df = spark.read.option("inferSchema", "true").parquet("hdfs://mnipdsnaga:9000/user/summer16rp2/masterdata/MN_ITEM_VER")
    //val input = "hdfs://mnipdgpng1:9000//user/informatica/parquet/INDIR_SALE_CLOSED/";
    val input = "input/file.parquet";

    val df = spark.read.option("inferSchema", "true").parquet(input)
    //val rdd = spark.sparkContext.textFile("pom.xml")

    df.show()
    df.printSchema()

    import com.databricks.spark.avro._
    df.write.partitionBy("customerId").avro("input/avr_"+System.currentTimeMillis())

//    val rdd = sc.parallelize(List("Hello", "world", "!"))
//
//    rdd.saveAsTextFile("hdfs://namenode_host:port/file/path");
//
//
//    df.write.save("hdfs://mnipdsnaga:9000/user/summer16rp2/map_ver2");

//    df.select($"cat_map_id", $"ver_start_date", $"ver_end_date").collect().map(println);

    //    df.select($"ver_end_date").collect().map(println)


  }
}

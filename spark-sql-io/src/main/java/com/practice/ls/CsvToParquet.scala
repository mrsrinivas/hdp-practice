package com.practice.ls

import org.apache.spark.sql.SparkSession

/**
  * Created by Srinivas Reddy Alluri on 27-10-2016.
  */
object CsvToParquet {

  def main(args: Array[String]) {

    /**
      * spark-submit --master yarn --deploy-mode cluster --class com.practice.ls.CsvToParquet <jar_location> <input_csv_path> <output_parquet_path>
      * */

    val session = SparkSession.builder
      .appName("CSV to Parquet Converter")
      .getOrCreate

    val df = session.read.format("org.apache.spark.sql.csv")
      .option("inferSchema", "true")
      .csv(args(0))


    df.write.parquet(args(1))

  }
}

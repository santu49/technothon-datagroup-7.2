package org.example

import org.apache.spark.sql.SparkSession

object demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataProject")
      .getOrCreate()

    // for ignoring warnings
    spark.sparkContext.setLogLevel("ERROR")

    //  read from a csv file
    val fileData=spark.read.option("header",true).csv("src/main/resources/csv_files/acc_16.csv")
    // only showing 20 rows
    fileData.show(20)

//    println( "Hello World!" )
  }

}

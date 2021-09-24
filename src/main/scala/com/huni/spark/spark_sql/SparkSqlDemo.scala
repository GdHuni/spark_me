package com.huni.spark.spark_sql

import org.apache.spark.sql.SparkSession

object SparkSqlDemo {

  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    //导包
    import spark.implicits._



    spark.close()
  }
}

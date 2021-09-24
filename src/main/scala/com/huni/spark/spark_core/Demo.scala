package com.huni.spark.spark_core

import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    sc.stop()
  }

}

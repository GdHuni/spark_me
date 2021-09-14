package com.huni.spark.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //从集合读取
    readOnArray(sc)

    //从文件读取的
   // readOnFile(sc)
    sc.stop()
  }

  private def readOnArray(sc: SparkContext) = {
    val rdd = sc.parallelize(Array("1, 2, 3, 4,5,1, 55,3, 4,7,6, 2, 4, 2,22"))
    // 检查 RDD 分区数
    val partitions = rdd.getNumPartitions
    println("partitions========="+partitions)

    val tuples = rdd.flatMap(_.split(",")).map(x => (x, 1)).reduceByKey((x,y) =>x+y).sortBy(_._2,false).foreach(println(_))

    }

  private def readOnFile(sc: SparkContext) = {
    // val lines: RDD[String] = sc.textFile("hdfs://linux121:9000/wcinput/wc.txt")
    val lines: RDD[String] = sc.textFile("D:\\IdeaProjects\\spark_me\\src\\main\\wcinput.txt")
    // val lines: RDD[String] = sc.textFile("data/wc.dat")
    lines.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect().foreach(println)
  }
}

package com.huni.spark.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapSideJoin {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapSideJoin")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    // 设置本地文件切分大小 默认本地文件切块大小为32M
    sc.hadoopConfiguration.setLong("fs.local.block.size", 128*1024*1024)
    // map task：数据准备
    val productRDD: RDD[(String, String)] = sc.textFile("data/lagou_product_info.txt")  .map {
      line => val fields = line.split(";")
        (fields(0), line)
    }

    val orderRDD: RDD[(String, String)] = sc.textFile("data/orderinfo.txt",8 ).map {
        line => val fields = line.split(";")
          (fields(2), line)
    }
    // join有shuffle操作
    val resultRDD = productRDD.join(orderRDD)
    println(resultRDD.count())
    Thread.sleep(1000000)
    sc.stop()
  }

}

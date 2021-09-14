package com.huni.spark.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable
import scala.util.Random

/**
 * 自定义分区器
 * 1. 继承 Partitioner，实现里面的变量及方法
 * 2.在方法中主动使用分区器， partitionBy(new MyPartitioner(n))
 */
class MyPartitioner(n :Int) extends Partitioner{
  //定义分区个数
  override def numPartitions: Int = n

  //根据key来实现自定义分区器
  override def getPartition(key: Any): Int = {
    val k = key.toString.toInt
    k/100
  }
}

object UserDefindPartitioner{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("myPartitioner")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //实现逻辑

    val random: Random.type = scala.util.Random
    val ints: immutable.IndexedSeq[Int] = (1 to 100).map(x => random.nextInt(1000))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(ints).map((_, 1))
    rdd1.glom.collect.foreach(x => println(x.toBuffer))
    println("===================================================")
    val rdd2 = rdd1.partitionBy(new MyPartitioner(10))

    rdd2.glom.collect.foreach(x => println(x.toBuffer))

    sc.stop()
  }
}


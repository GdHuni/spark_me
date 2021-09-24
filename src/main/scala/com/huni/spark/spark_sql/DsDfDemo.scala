package com.huni.spark.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StructType

import java.lang

case class Person(name:String, age:Int, height:Int)

object DsDfDemo {
  def main(args: Array[String]): Unit = {
    //DataFrame(DataFrame = RDD[Row] + Schema):
    //Dataset(Dataset = RDD[case class].toDS):

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    //导包
    import spark.implicits._
    //创建schema
    val schema: StructType = new StructType()
      .add("name", "string", false)
      .add("age", "int", false)


    //==============================通过range创建ds=====================================
    val numDS: Dataset[lang.Long] = spark.range(5, 100, 5)

    // orderBy 转换操作；desc：function；show：Action
    numDS.orderBy(desc("id")).show(5)
    // 显示schema信息
    numDS.printSchema

    // 使用RDD执行同样的操作
    println(numDS.rdd.map(_.toInt).stats)

    // 检查分区数
    val partitions: Int = numDS.rdd.getNumPartitions
    println(partitions)

    //==============================通过集合创建ds=====================================


    val persons = List(Person("zhan", 18, 10), Person("lisi", 19, 22))
    val rdd: RDD[Person] = sc.makeRDD(persons)
    val ds: Dataset[Person] = rdd.toDS()
    val ds1 = spark.createDataset(persons)
    val ds2 = spark.createDataset(rdd)
    ds.show
    ds1.show
    ds2.show

    //==============================通过集合创建ds=====================================
    val lst = List(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val df1: DataFrame = spark.createDataFrame(lst)
      .withColumnRenamed("_1", "name1")
      .withColumnRenamed("_2", "age1")
      .withColumnRenamed("_3", "height1")
    df1.orderBy("age1").show(10)

    // 修改整个DF的列名
    val df2 = spark.createDataFrame(lst).toDF()
    val frame: DataFrame = lst.toDF()
    df2.orderBy("age").show(10)
    frame.orderBy("age").show(10)





    spark.close()


  }

}

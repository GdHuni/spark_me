package com.huni.spark.spark_sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//case class Person(name:String, age:Int, height:Int)

object Demo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    import spark.implicits._
//    val arr2 = Array(("Jack", 28, 150), ("Tom", 10, 144), ("Andy", 16, 165))
//    val rddToDF: DataFrame = sc.makeRDD(arr2).toDF("name", "age", "height")
//    rddToDF.orderBy("age").show(10)
//    rddToDF.orderBy(desc("age")).show(10)

//    val arr2 = Array(("Jack", 28, 150), ("Tom", 10, 144), ("Andy", 16, 165))
//    val rdd2: RDD[Person] = spark.sparkContext.makeRDD(arr2).map(f=>Person(f._1, f._2, f._3))
//    val ds2 = rdd2.toDS()			// 反射推断，spark 通过反射从case class的定义得到类名
//    val df2 = rdd2.toDF()			// 反射推断
//    ds2.printSchema
//    df2.printSchema
//    ds2.orderBy(desc("name")).show(10)
//    df2.orderBy(desc("name")).show(10)

    val df1: DataFrame = spark.read.csv("data/people1.csv")
    //D:\IdeaProjects\spark_me-git\src\main\resources\data
    df1.printSchema()
    df1.show()

    val df2: DataFrame = spark.read.csv("data/people2.csv")
    df2.printSchema()
    df2.show()

    // 定义参数
    val df3: DataFrame = spark.read
        .options(Map(("header", "true"), ("inferschema", "true")))
      .csv("data/people1.csv")
    df3.printSchema()
    df3.show()

    // Spark 2.3.0
    val schemaStr = "name string, age int, job string"
    val df4: DataFrame = spark.read
        .option("header", "true")
      .option("delimiter", ";")
      .schema(schemaStr)
      .csv("data/people2.csv")
    df4.printSchema()
    df4.show()

    spark.close()
  }
}
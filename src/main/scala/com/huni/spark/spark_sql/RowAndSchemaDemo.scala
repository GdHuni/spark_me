package com.huni.spark.spark_sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * @Classname RowAndSchemaDemo
 * @Description row schema信息描述
 * @Date 2021/9/26 18:26
 * @Created by huni
 */
object RowAndSchemaDemo {
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
    //=======================================Row是一个泛化的无类型 JVM object=======================================
    val row1 = Row(1,"abc", 1.2)
    //row  几种取数方式
    println(row1(0))
    println(row1.getString(1))
    println(row1.getAs[Double](2))

    //=======================================创建schema的几种方式=======================================
    val schema1 = StructType(
      StructField("name", StringType, false)
      :: StructField("age", IntegerType, false)
      :: StructField("height", IntegerType, false)
      :: Nil)

    val schema2 = StructType(
      Seq(StructField("name", StringType, false)
        ,StructField("age", IntegerType, false)
        , StructField("height", IntegerType, false)
      )
    )

    val schema3 = StructType(
      List(
        StructField("name", StringType, false)
        ,StructField("age", IntegerType, false)
        ,StructField("height", IntegerType, false)
      )
    )

    val schema4 = (new StructType)
      . add(StructField("name", StringType, false))
      . add(StructField("age", IntegerType, false))
      . add(StructField("height", IntegerType, false))

    val schema5 = new StructType()
      .add("id", "int", false)
      .add("name", "string", false)
      .add("height", "double", false)



    //读取csv
    spark.read.csv()

    spark.close()
  }

}

package com.huni.spark.spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Classname ActionDemo
 * @Description DataFram和DataSet action算子的demo案例
 *              show、collect、collectAsList、head、first、count、take、takeAsList、reduce
 * @Date 2021/9/26 18:26
 * @Created by huni
 */
object ActionDemo {
  def main(args: Array[String]): Unit = {

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    //导包
    //import spark.implicits._

    val df1: DataFrame = spark.read.option("header", "true")
      .option("inferschema", "true")
      .csv("data/emp.dat")
    // show、collect、collectAsList、head、first、count、take、takeAsList、reduce
    val count: Long = df1.count()
    println(s"df1.count(): $count")
    df1.show()
    // union == union all  求并集，不去重
    println(df1.union(df1).count())

    // 截断字符(展示...)
    df1.toJSON.show()
    // 显示10行，不截断字符
    df1.toJSON.show(10,false)

    spark.catalog.listFunctions.show(100, false)

    // collect返回的是数组, Array[org.apache.spark.sql.Row]
    val c1 = df1.collect()
    println(c1)
    // collectAsList返回的是List, List[org.apache.spark.sql.Row]
    val c2 = df1.collectAsList()
    println(c2)
    // 返回 org.apache.spark.sql.Row val h1 = df1.head()
    val f1 = df1.first()
    println(f1)
    // 返回 Array[org.apache.spark.sql.Row]，长度为3
    val h2 = df1.head(3)
    println(h2)

    val f2 = df1.take(3)
    f2.foreach(println(_))
    // 返回 List[org.apache.spark.sql.Row]，长度为2
    val t2 = df1.takeAsList(2)
    println(t2)

    // 结构属性
    // 查看列名
    println( df1.columns.toList)
    // 查看列名和类型
    println(  df1.dtypes.toMap)
    // 参看执行计划
    println( df1.explain())
    // 获取某个列
    println(df1.col("ename"))

     // 常用
    println(df1.printSchema)

    spark.sql(
      """
        |
        |""".stripMargin)
    //关闭资源
    spark.close()


  }

}

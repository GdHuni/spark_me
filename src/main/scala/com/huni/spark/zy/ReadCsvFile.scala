package com.huni.spark.zy

import org.apache.spark.sql.{DataFrame, SparkSession}


object ReadCsvFile {
  def main(args: Array[String]): Unit = {
    //初始化SparkSession  是DataSet和DataFrame编写Spark程序的入口
    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .master("local[1]")
      .getOrCreate()
    //创建上下文环境
    val sc = spark.sparkContext
    //日志级别
    sc.setLogLevel("warn")

    //读取数据文件并且 自动识别字段类型
    val df: DataFrame = spark.read.option("inferschema", "true").csv("D:\\A_E\\大数据兼职\\交付4\\data.csv")

/*  指定数据schema
    val schema = "name string, age int, job string"
    val df: DataFrame = spark.read.option("inferschema", "true").schema(schema).csv("D:\\A_E\\大数据兼职\\交付4\\data.csv")
*/

    //将文件数据创建成虚拟表 并展示
    df.createTempView("t")
    //需求1 过滤出当第三列字段的值为null或wasm或nan时，选取对应的第二列字段的值
    spark.sql(
      """
        | select sum(_c1)/86400 from t where _c2 in ('null','wasm','nan')
        |""".stripMargin).rdd.map(_.getDouble(0)).saveAsTextFile("D:\\out0")

    spark.sql(
      """
        | select count(*) from t where _c2 in ('null','wasm','nan')
        |""".stripMargin).rdd.map(_.getLong(0)).saveAsTextFile("D:\\A_E\\大数据兼职\\交付4\\out1")

    //需求2 分别计算前两列乘积，再求之和 并展示
    spark.sql(
      """
        | select sum(_c0*_c1) from t
        |""".stripMargin).rdd.map(_.getLong(0)).saveAsTextFile("D:\\A_E\\大数据兼职\\交付4\\out2")

    spark.close()
  }
}
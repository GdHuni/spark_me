package com.huni.spark.spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StructType

import java.lang

case class Person(name:String, age:Int, height:Int)
/**
 * @Classname RowAndSchemaDemo
 * @Description DataFram和DataSet创建的demo案例
 * @Date 2021/9/26 18:26
 * @Created by huni
 */
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
    val ds = spark.createDataset(persons)
    ds.show


    // Dataset = RDD[case class]  DataFrame = RDD[Row] + Schema
    //================================RDD转Dataset================================
    val rdd: RDD[Person] = sc.makeRDD(persons)
    // 反射推断，spark 通过反射从case class的定义得到类名
    val ds1: Dataset[Person] = rdd.toDS()
    val ds2 = spark.createDataset(rdd)
    ds1.show
    ds2.show


    //==============================通过集合创建df=====================================
    val lst = List(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))
    val df1: DataFrame = spark.createDataFrame(lst)
      .withColumnRenamed("_1", "name1")
      .withColumnRenamed("_2", "age1")
      .withColumnRenamed("_3", "height1")
    df1.orderBy("age1").show(10)

    // 修改整个DF的列名
    val df2 = spark.createDataFrame(lst).toDF("name","age","height")
    val frame: DataFrame = lst.toDF("name","age","height")
    df2.orderBy("age").show(10)
    frame.orderBy("age").show(10)


    //==============================RDD 转成 DataFrame=====================================
    val arr = Array(("Jack", 28, 184), ("Tom", 10, 144), ("Andy", 16, 165))

    //创建RDD[Row]
    val rdd3: RDD[Row] = sc.makeRDD(arr).map(r => Row(r._1, r._2, r._3))

    //创建schema,名称、类型、是否可以为nul
    val schema3: StructType = new StructType()
      .add("name", "string",false)
      .add("age", "int",false)
      .add("height", "int",false)

    val df3: DataFrame = spark.createDataFrame(rdd3, schema3)
    df3.orderBy(desc("name")).show()
    println("============")

    //非 RDD[Row]的RDD可以 调用todDF函数 import spark.implicits._ 隐式转换里的
    val rdd4: RDD[(String, Int, Int)] = sc.makeRDD(arr)
    // 反射推断
    rdd4.toDF("name4","age4","height4").show()

    //==============================从文件创建DateFrame(以csv文件为例)=====================================
    //默认的,分割 不加头信息(第一行是否作为列名)
    val df5: DataFrame = spark.read
      .csv("data/people1.csv")
    df5.printSchema()
    df5.show()
    // 指定参数  spark 2.3.0以上才行
    val schema6 = "name string, age int, job string"
    val df6: DataFrame = spark.read
      .option("header","true")
      .option("delimiter",";")
      .schema(schema6)
      .csv("data/people2.csv")
    df6.printSchema()
    df6.show()

    // 自动类型推断
    val df7: DataFrame = spark.read
      .option("header","true")
      .option("delimiter",";")
      .option("inferschema","true")
      .csv("data/people2.csv")
    df7.printSchema()
    df7.show()

    //===============================df、ds、rdd转换关系===============================
    //ds ==> df,rdd
    ds.toDF()
    ds.rdd
    //df ==> ds,rdd
    case class Coltest(col1:String,col2:Int)extends Serializable //定义字段名和类型
    df7.as[Coltest]
    df7.rdd

    //rdd ==> ds,df //Dataset = RDD[case class]  DataFrame = RDD[Row] + Schema
    //关闭资源
    spark.close()


  }

}

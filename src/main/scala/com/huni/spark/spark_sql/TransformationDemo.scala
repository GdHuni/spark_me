package com.huni.spark.spark_sql

import com.sun.openpisces.Dasher
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel


/**
 * @Classname ActionDemo
 * @Description DataFram和DataSet action算子的demo案例
 *              show、collect、collectAsList、head、first、count、take、takeAsList、reduce
 * @Date 2021/9/26 18:26
 * @Created by huni
 */
object TransformationDemo {
  //创建sparkSession
  private val spark: SparkSession = SparkSession.builder()
    .appName("demo")
    .master("local[*]")
    .getOrCreate()
  private val sc = spark.sparkContext
  sc.setLogLevel("warn")
  //导包 隐式转换
  import spark.implicits._


  def main(args: Array[String]): Unit = {
   // emp.dat
   val df1: DataFrame = spark.read
     .option("header","true")
     .option("inferschema","true")
     .csv("data/emp.dat")
   // println(df1.count())
    //df1.show()

    //类似rdddemo
   // RddDemo(df1)

    //存储相关 cacheTable、persist、checkpoint、unpersist、cache
    //cacheDemo(df1)

    //select相关
    //selectDemo(df1)

    // drop、withColumn、 withColumnRenamed 返回的是DF
    //dropDemo(df1)

    // where相关  where == filter
    //whereDemo(df1)

    // groupBy相关 groupBy、agg、max、min、avg、sum、count（后面5个为内置函数）
    //groupByDemo(df1)

    //orderBy相关 spark的 orderBy == sort
    //orderByDemo(df1)


    val lst = List(StudentAge(1, "Alice", 18), StudentAge(2, "Andy", 19), StudentAge(3, "Bob", 17), StudentAge(4, "Justin", 21), StudentAge(5, "Cindy", 20))
    val ds1 = spark.createDataset(lst)
    ds1.show()
    // 定义第二个数据集
    val rdd = sc.makeRDD(List(StudentHeight("Alice", 160, 19), StudentHeight("Andy", 159, 19), StudentHeight("Bob", 170, 18), StudentHeight("Cindy", 165, 19),
      StudentHeight("Rose", 160, 17)))
    val ds2: Dataset[StudentHeight] = rdd.toDS

    //join相关
    // joinDemo(df1,ds1,ds2)

    //集合相关 union==unionAll（过期）、intersect、except 以及null值处理
    //nullDemo(df1, ds1, ds2)

    //窗口函数  一般情况下窗口函数不用 DSL 处理，直接用SQL更方便
    //参考源码Window.scala、WindowSpec.scala（主要）
    import org.apache.spark.sql.expressions.Window
    val w1 = Window.partitionBy("JOB").orderBy("hiredate")
    val w2 = Window.partitionBy("JOB").orderBy("sal")
    val w3 = w1.rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val w4 = w1.rowsBetween(-1, 1)
    // 聚组函数【用分析函数的数据集】
    df1.orderBy($"job",$"sal").show()
    df1.select($"job", $"sal", sum("sal").over(w1).alias("sal1")).show
    df1.select($"job", $"sal", sum("sal").over(w3).alias("pv1")).show
    df1.select($"job", $"sal", sum("sal").over(w4).as("pv1")).show
    // 排名
    df1.select($"job", $"sal", rank().over(w2).alias("rank")).show
    df1.select($"job", $"sal", dense_rank().over(w2).alias("denserank")).show
    df1.select($"job", $"sal", row_number().over(w2).alias("rownumber")).show
    // lag、lead
    df1.select($"job", $"sal", lag("sal", 2).over(w2).alias("rownumber")).show
    df1.select($"job", $"sal", lag("sal", -2).over(w2).alias("rownumber")).show

    df1.createOrReplaceTempView("emp")
    spark.sql(
      """
        |select job,sal,sum(sal) over(partition by job ORDER by sal ) from emp
        |""".stripMargin)

    //关闭资源
    spark.close()


  }

  private def nullDemo(df1: DataFrame, ds1: Dataset[StudentAge], ds2: Dataset[StudentHeight]): Unit = {
    // union、unionAll、intersect、except。集合的交、并、差

    val ds3 = ds1.select("name")
    val ds4 = ds2.select("sname")
    // union 求并集，不去重
    ds3.union(ds4).show
    // unionAll、union 等价；unionAll过期方法，不建议使用
    ds3.unionAll(ds4).show

    // intersect 求交
    ds3.intersect(ds4).show
    // except 求差
    ds3.except(ds4).show
    //空值处理
    //na.fill、na.drop

    // NaN (Not a Number)
    math.sqrt(-1.0)
    math.sqrt(-1.0).isNaN()
    df1.show

    // 删除所有列的空值和NaN
    df1.na.drop.show

    // 删除某列的空值和NaN
    df1.na.drop(Array("mgr")).show

    // 对全部列填充；对指定单列填充；对指定多列填充
    df1.na.fill(1000).show
    df1.na.fill(1000, Array("comm")).show
    df1.na.fill(Map("mgr" -> 2000, "comm" -> 1000)).show

    // 对指定的值进行替换
    df1.na.replace("comm" :: "deptno" :: Nil, Map(0 -> 100, 10 -> 100)).show

    // 查询空值列或非空值列。isNull、isNotNull为内置函数
    df1.filter("comm is null").show
    df1.filter($"comm".isNull).show
    df1.filter(col("comm").isNull).show
    df1.filter("comm is not null").show
    df1.filter(col("comm").isNotNull).show
  }

  private def joinDemo(df1: DataFrame, ds1:Dataset[StudentAge], ds2:Dataset[StudentHeight]): Unit = {
    //    Inner Join : 内连接；
    //    Full Outer Join : 全外连接；
    //    Left Outer Join : 左外连接；
    //    Right Outer Join : 右外连接；
    //    Left Semi Join : 左半连接；
    //    Left Anti Join : 左反连接；
    //    Natural Join : 自然连接；
    //    Cross (or Cartesian) Join : 交叉 (或笛卡尔) 连接
    // 这里解释一下左半连接和左反连接，这两个连接等价于关系型数据库中的 IN 和 NOT IN 字句：

    //    -- LEFT SEMI JOIN
    //      SELECT * FROM emp LEFT SEMI JOIN dept ON emp.deptno = dept.deptno
    //    -- 等价于如下的 IN 语句
    //      SELECT * FROM emp WHERE deptno IN (SELECT deptno FROM dept)
    //
    //    -- LEFT ANTI JOIN
    //      SELECT * FROM emp LEFT ANTI JOIN dept ON emp.deptno = dept.deptno
    //    -- 等价于如下的 IN 语句
    //      SELECT * FROM emp WHERE deptno NOT IN (SELECT deptno FROM dept)

    //笛卡尔积
    println(df1.crossJoin(df1).count)
    // 2、等值连接（单字段）（连接字段empno，仅显示了一次）
    df1.join(df1, "empno").show()
    // 3、等值连接（多字段）（连接字段empno、ename，仅显示了一次）
    df1.join(df1, Seq("empno", "ename")).show

    ds1.createOrReplaceTempView("ds1")
    ds2.createOrReplaceTempView("ds2")

    // 备注：不能使用双引号，而且这里是 ===
    ds1.join(ds2, $"name" === $"sname").show
    //当两个表的字段一样的时候，需要用 下面的方式
    ds1.join(ds2, ds1.col("age") === ds2.col("age")).show
    ds1.join(ds2, ds1("age") === ds2("age")).show
    // 等价 SQL 如下：
    spark.sql("select * from ds1  join ds2 on ds1.age = ds2.age").show()

    // 报错 Constructing trivially true equals predicate, ''age = 'age'. Perhaps you need to use aliases.  Reference 'age' is ambiguous, could be: age, age.;
    //ds1.join(ds2, $"age" === $"age").show
    ds1.join(ds2, $"name" === $"sname", "outer").show
    ds1.join(ds2, $"name" === $"sname", "full").show
    ds1.join(ds2, $"name" === $"sname", "full_outer").show
  }

  case class StudentAge(sno: Int, name: String, age: Int)
  case class StudentHeight(sname: String, height: Int,age: Int)


  private def RddDemo(df1: DataFrame): Unit = {
    df1.printSchema()
    df1.map(row=>row.getAs[Int](0))
    // randomSplit(与RDD类似，将DF、DS按给定参数分成多份)
    val df2 = df1.randomSplit(Array(0.1, 0.1, 0.7,0.3))
    println(df2(0).count)
    println(df2(1).count)
    println(df2(2).count)
    // 取10行数据生成新的DataSet
    val df3: Dataset[Row] = df1.limit(10)
    val df4: Dataset[Row] = df3.union(df1)
    //去重
    df4.distinct()

    //dropDuplicates，按列值去重(没指定就是所有的列)
    df3.dropDuplicates().show()
    df3.dropDuplicates("deptno").show()

    // 返回全部列的统计（count、mean、stddev、min、max）
    df1.describe().show

    // 返回指定列的统计
    df1.describe("empno", "comm").show
  }

  private def cacheDemo(df1: DataFrame): Unit = {
    spark.sparkContext.setCheckpointDir("D:\\tmp\\checkpoint")
    df1.persist(StorageLevel.MEMORY_ONLY)
    df1.cache()
    df1.checkpoint()
    df1.unpersist(true)

    df1.createOrReplaceTempView("t1")
    spark.sql("select * from t1").show
    spark.catalog.cacheTable("t1")
    spark.catalog.uncacheTable("t1")
  }

  private def selectDemo(df1: DataFrame): Unit = {
    // 列的多种表示方法。使用""、$""、'、col()、ds("")
    df1.select($"sal").show()

    // 下面的写法无效，其他列的表示法有效
    //df1.select("ename", "hiredate", "sal"+100).show
    //df1.select("ename", "hiredate", "sal+100").show
    // 这样写才符合语法
    df1.select($"ename", $"hiredate", $"sal" + 100).show
    df1.select('ename, 'hiredate, 'sal + 1002).show
    df1.select( col("sal") + 1002,(df1.col("sal")+1003).as("sal1"),df1("sal")+1004).show

    // 可使用expr表达式(expr里面只能使用引号)
    df1.select(expr("comm+100"), expr("sal+100"), expr("ename")).show
    df1.selectExpr("ename", "hiredate", "sal+1001").show()
    df1.selectExpr("round(sal, -3) as newsal", "sal", "ename").show
    // cast，类型转换
    df1.selectExpr("cast(empno as string)").printSchema

    df1.select($"empno".cast(IntegerType)).printSchema
  }

  private def dropDemo(df1: DataFrame): Unit = {
    // drop 删除一个或多个列，得到新的DF
    df1.drop("mgr")
    df1.drop("empno", "mgr")
    // withColumn，修改列值

    val df2 = df1.withColumn("sal", $"sal" + 1000)
    df2.show
    // withColumnRenamed，更改列名
    val df3: DataFrame = df1.withColumnRenamed("sal", "newsal")
    df3.show()
  }

  private def whereDemo(df1: DataFrame): Unit = {
    df1.where("sal> 1000 and job=='MANAGER'").show()
  }

  private def groupByDemo(df1: DataFrame): Unit = {
    // groupBy、max、min、mean、sum、count（与df1.count不同)
    df1.groupBy("deptno").sum("sal").show

    df1.groupBy("Job").agg(max("sal"), min("sal"), avg("sal"), sum("sal"), count("sal")).show

    // 给列取别名
    df1.groupBy("Job").agg(max("sal"), min("sal"), avg("sal"), sum("sal"), count("sal")).withColumnRenamed("min(sal)", "min1").show
    df1.groupBy("Job").agg(max("sal").as("max1"), min("sal").as("min2"), avg("sal").as("avg3"), sum("sal").as("sum4"), count("sal").as("count5")).show
  }

  private def orderByDemo(df1: DataFrame): Unit = {
    df1.where("JOB == 'CLERK'").orderBy($"sal").select($"sal", $"ENAME").show()
    //倒序
    df1.orderBy(-$"sal").show()
    df1.orderBy($"sal".desc).show()
    // sort，以下语句等价
    df1.sort("sal").show
    df1.sort($"sal").show
    df1.sort($"sal".asc).show
    df1.sort('sal).show
    df1.sort(col("sal")).show
    df1.sort(df1("sal")).show

    df1.sort($"sal".desc).show
    df1.sort(-'sal).show
    df1.sort(-'deptno, -'sal).show
  }
}

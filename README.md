# spark_me
spark study project
这是我刚开始学习spark的练手项目，里面会有许多的demo案例，会持续更新。
有助于小白开始着手学习spark，希望对你有所帮助

sparkSql 创建模板
```
  //创建sparkSession
  val spark: SparkSession = SparkSession.builder()
      .appName("demo")
      .master("local[*]")
      .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("warn")
  //导包
  import spark.implicits._

  //关闭资源
  spark.close()
```

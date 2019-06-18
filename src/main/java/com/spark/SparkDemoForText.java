package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @功能描述: 读取文件数据来计算单词个数
 * @项目版本: 1.0.0
 * @项目名称: sparkDemo
 * @相对路径: com.spark.SparkDemoForText
 * @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
 * @创建日期: 2019/6/17 14:19
 */
public class SparkDemoForText {
    /* 打包到服务器后的执行语句：
         ./spark-submit --class com.spark.SparkDemoForText  --master spark://master:7077 /tmp/spark_me-1.0-SNAPSHOT.jar
         hdfs://master:9000/test hdfs://master:9000/out111*/
    public static void main(String[] args) {
        //创建java版本的sparkcontext
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("myApp");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取数据
//        JavaRDD<String> rdd = sc.textFile("d://test.txt");
        JavaRDD<String> rdd = sc.textFile(args[0]);
        //切分单词
        JavaRDD<String> words = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        // 转换为键值对并计数
        JavaPairRDD<String,Integer> counts = words.mapToPair(s -> new Tuple2<>(s,1)).reduceByKey((i,i1) -> i+i1);
        //输出到文本中
//        counts.saveAsTextFile("d://shuchu");
        counts.saveAsTextFile(args[1]);
        sc.stop();
    }
}

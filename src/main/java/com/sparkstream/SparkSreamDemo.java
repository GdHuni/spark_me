package com.sparkstream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @功能描述:
 * @项目版本:
 * @相对路径: com.sparkstream.SparkSreamDemo
 * @创建作者: huni
 * @创建日期: 2019/3/31 20:52
 */
public class SparkSreamDemo {
    public static void main(String[] args) throws InterruptedException {
        //local后面参数必须设置大于1，因为要多线程去处理，只有1的话他就只会运行接收器，而无法运行处理器了（接收器一直运行）
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("demo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        //设置接收时间间隔
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(3));
        //创建一个将要连接到hostname:port 的离散流（可以在虚拟机上使用 nc -lk 7777 命令）
        JavaReceiverInputDStream<String> lines =
                ssc.socketTextStream("master", 7777);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        /*JavaPairDStream<String, Integer> counts =
                lines.flatMap(x->Arrays.asList(x.split(" ")).iterator())
                        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                        .reduceByKey((z, y) -> z + y);*/

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        // 启动计算
        ssc.start();
        ssc.awaitTermination();

    }
}

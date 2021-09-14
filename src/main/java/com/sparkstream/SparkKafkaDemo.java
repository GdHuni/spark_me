package com.sparkstream;/*
package com.sparkstream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

*/
/**
 * @功能描述: spark连接Kafak的demo案例
 * @项目版本:
 * @相对路径: com.sparkstream.SparkKafkaDemo
 * @创建作者: huni
 * @创建日期: 2019/3/31 21:04
 *//*

public class SparkKafkaDemo {
    public static void main(String[] args) {
        String brokers = "master:9092";
        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("streaming word count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        Collection<String> topics = Arrays.asList("new");//设置kafka的主题
        //设置kafka的参数
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("auto.offset.reset", "latest");//会从开启消费者之后开始消费kafka,earliest会从头开始消费
        kafkaParams.put("enable.auto.commit", false);//是否自动提交offs
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "newgroup");//设置消费者组
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //设置该参数并且设置ConsumerStrategies.Subscribe(topics,kafkaParams,offsets)spark就会重复消费kafka(一直从头开始)
        */
/*Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("new", 1), 2L);*//*


        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        //这里就跟之前的demo一样了，只是需要注意这边的lines里的参数本身是个ConsumerRecord对象
        JavaPairDStream<String, Integer> counts =
                lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
                        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                        .reduceByKey((x, y) -> x + y);
        counts.print();//打印信息

        lines.foreachRDD(rdd -> {
            //保存偏移量
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            ((CanCommitOffsets) lines.inputDStream()).commitAsync(offsetRanges);
            //获取偏移量
          */
/*  rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });*//*

        });

        lines.foreachRDD(rdd -> {
            rdd.foreach(x -> {
                System.out.println(x);
            });
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ssc.close();

    }
}
*/

package com.sjyttkl.bigdata.Spark_streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming04_KafkaSource {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3))

    //createStream(SparkStream上下文，zookeeper地址，kafka消费者组，Map(kafka主题->该主题分区数)),
    //创建的kafkaDStream是[String,String]类型的，第一个String是每条消息的key，第二个String是每个消息的value
    // Map("xx"->3)这是scala的key-value写法
    var kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "linux1:2181", "xiaodong", Map("xiaodong" -> 3))

    //扁平化。这里t =>t._2.split(),为什么要t._2呢？因为kafka生产消费数据的数据都是key-value对，只是默认key是null我们没关注，但事实上只有它的value才是一行行消息。
    var WordDstream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))

    //格式转化
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))

    //聚合
    var wordToSumStream: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)

    //打印结果
    wordToSumStream.print()

    //启动采集器
    streamingContext.start()
    streamingContext.awaitTermination()
    //然后启动kafka生产者往指定topic发数据即可看到测试效果
  }
}


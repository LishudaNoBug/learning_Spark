package com.sjyttkl.bigdata.Spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create with: com.sjyttkl.bigdata.Spark_streaming 
  * author: sjyttkl
  * E-mail:  695492835@qq.com
  * date: 2019/9/29 12:53
  * version: 1.0
  * description:  窗口操作
  */
object SparkStreaming06_window {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3))

    //设置检查点，保存之前已经wordCount的次数
    streamingContext.sparkContext.setCheckpointDir("cp")

    //kafka数据源
    var kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "linux1:2181", "xiaodong", Map("xiaodong" -> 3))

    //窗口大小应该为采集周期的整数倍，窗口滑动步长也应该是采集周期的整数倍【核心改动】
    var windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    //flatMap扁平化
    var WordDstream: DStream[String] = windowDStream.flatMap(t => t._2.split(" "))
    //map转换格式
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))

    //updateStateByKey有状态操作
    var stateDStream: DStream[(String, Int)] = mapDstream.updateStateByKey {
      case (seq, buffer) => {
        var sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    stateDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}


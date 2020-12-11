package com.sjyttkl.bigdata.Spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create with: com.sjyttkl.bigdata.Spark_streaming 
  * author: sjyttkl
  * E-mail:  695492835@qq.com
  * date: 2019/9/25 10:12 
  * version: 1.0
  * description:  
  */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //    //使用SparkStreaming 完成WordCount
    //    SparkSubmit

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    //十秒钟收集一次
    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(10))
    //输入源为socketTextStream：网络端口；socketLineStreaming内是十秒钟收集到的多行数据。每一行是一个String
    var socketLineStreaming: ReceiverInputDStream[String] = streamingContext.socketTextStream("www.bestocreate.com", 2202) //一行一行的接受

    //数据处理   flatmap扁平化==》map加键==》reduceByKey聚合
    var WordDstream: DStream[String] = socketLineStreaming.flatMap(line => line.split(" "))
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))
    var wordToSumStream: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)

    //DStream可以直接print()
    wordToSumStream.print()

    //启动采集器，此采集器会一直运行。
    //Drvier等待采集器停止才停。
    // 不加这两行采集器不会开始采集，Driver拿不到数据，程序就结束了。
    streamingContext.start()
    streamingContext.awaitTermination()

    //linux往22端口发数据：   nc -lc 22
  }
}

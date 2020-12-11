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
  * description:  transform
  */
object SparkStreaming07_transform {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3))
    //通过socket拿数据
    var socketLineStreaming: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux1", 9999) //一行一行的接受


    //Map：
    //TODO 代码（Driver)
    socketLineStreaming.map {
      case x => {
        //TODO 代码（Executer)
        x
      }
    }

    //transform：
    //    //TODO 代码（Driver)
    //    socketLineStreaming.transform{
    //      case rdd=>{
    //        //TODO 代码（Driver)(m  运行采集周期 次)
    //        rdd.map{
    //          case x=>{
    //            //TODO 代码 （Executer)
    //            x
    //          }
    //        }
    //      }
    //    }


    //socketLineStreaming.print()可以删除，但是foreachRDD会遍历DS中的所有RDD这样就能使用RDD的方法
    socketLineStreaming.foreachRDD(rdd => {
      rdd.foreach(println)
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}


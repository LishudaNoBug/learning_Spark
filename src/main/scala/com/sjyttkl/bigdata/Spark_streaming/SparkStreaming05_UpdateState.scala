package com.sjyttkl.bigdata.Spark_streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create with: com.sjyttkl.bigdata.Spark_streaming 
  * author: sjyttkl
  * E-mail:  695492835@qq.com
  * date: 2019/9/29 12:53
  * version: 1.0
  * description:  有状态的数据转换
  */
object SparkStreaming05_UpdateState {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3)) //StreamingContext3秒钟采集一次数据

    //保存数据的状态，设置检查点路径【核心改动】
    streamingContext.sparkContext.setCheckpointDir("cp")

    //从kafka中采集数据
    var kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "linux1:2181", "xiaodong", Map("xiaodong" -> 3))

    //flatMap扁平化
    var WordDstream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))
    //map格式转化。mapDstream：（hello，1）（hello，1）（world，1）
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))

    //updateStateByKey方法返回一个包裹着结果的Option（只能在key-value类型的DStream上调用）【核心改动】
    //case (seq, buffer) => {     ：seq是指相同key的value集合例如3x(hello,1)那么hello的seq是（1,1,1），buffer是指中间处理数据的那个阶段
    //        var sum = buffer.getOrElse(0) + seq.sum      ：如果buffer是第一次计算那么赋初值为0，然后加上参数的seq总和就是1+1+1就是3
    //        Option(sum)
    //      }
    //返回的stateDStream[(String,int)]:String是key，Int是key出现的次数
    var stateDStream: DStream[(String, Int)] = mapDstream.updateStateByKey {
      case (seq, buffer) => {
        var sum = buffer.getOrElse(0) + seq.sum
        Option(sum) //将sum结果包裹在option里返回给buffer。我猜各个分区最终还会将option汇总执行与seq相同的.sum操作。然后保存到检查点，后面还会取出来继续操作。
      }
    }

    stateDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}


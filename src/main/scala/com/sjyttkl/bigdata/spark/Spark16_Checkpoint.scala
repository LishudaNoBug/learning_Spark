package com.sjyttkl.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/8 11:34
  * version: 1.0
  * description: RDD中的函数传递
  */
object Spark16_Checkpoint {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)

    //设置checkPoint检查点的保存目录，不设置会报错
    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    //将  mapRDD  设立一个检查点
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.foreach(println)

    //toDebugString方法打印血缘关系
    /*
    (12) ShuffledRDD[2] at reduceByKey at Spark16_Checkpoint.scala:25 []
    +-(12) MapPartitionsRDD[1] at map at Spark16_Checkpoint.scala:23 []
    |   ReliableCheckpointRDD[3] at foreach at Spark16_Checkpoint.scala:28 []
     */
    println(reduceRDD.toDebugString)

    sc.stop()
  }
}
package com.sjyttkl.bigdata.spark

import java.sql.PreparedStatement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/8 11:34
  * version: 1.0
  * description: 累加器
  */
//使用累加器来共享变量，来累加数据
object Spark20_ShareData {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //创建累加器对象
    var accumulator: LongAccumulator = sc.longAccumulator
    dataRDD.foreach {
      //各个分区的值使用同一个累加器，累加器在各个Execute内部累加完毕后会返回给Driver
      case i => {
        accumulator.add(i)
      }
    }
    //获取最终累加器的值  sum =10
    print("sum =" + accumulator.value)
    sc.stop()
  }
}
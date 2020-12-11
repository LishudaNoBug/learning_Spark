package com.sjyttkl.bigdata.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/17 11:34
  * version: 1.0
  * description: 自定义累加器
  */
object Spark21_Accumulator {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop", "hive", "Habsde", "scala", "hello"), 2)

    //自定义的累加器创建完需要注册
    val wordAccumulator = new WordAccumulator
    sc.register(wordAccumulator)

    //执行累加器的累加功能
    dataRDD.foreach {
      case word => {
        wordAccumulator.add(word)
      }
    }
    //获取最终累加器的值
    //[hive, hello]
    print(wordAccumulator.value)
    sc.stop()
  }
}


/*
自定义累加器
 1. 继承一个累加器抽象类AccumulatorV2 。
 class AccumulatorV2[IN, OUT]：第一个参数In指的是每个Execute内部往累加器里写的数据类型。
                               第二个参数OUT指的是Execute返回给Driver或者说Driver最终写出去的数据类型。
 2. 实现抽象方法，最重要的是add和value方法。
 */
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  //当前累加器是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //向累加器中增加数据，可以自定义累加规则
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }
}
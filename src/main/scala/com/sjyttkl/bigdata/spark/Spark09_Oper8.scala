package com.sjyttkl.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create with: com.sjyttkl.bigdata.spark 
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/8/25 0:59 
  * version: 1.0
  * description:  
  */
object Spark09_Oper8 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    var sc: SparkContext = new SparkContext(config)

    var listRDD: RDD[Int] = sc.makeRDD(1 to 16)

    /*
    第一个参数：  true：放回抽样，false不放回抽样
    第二个参数：  可理解为抽取40%的数据
    第三个参数：  随机数种子，如果种子一样那么随机出来的数一定一样
     */
    var SampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)
    SampleRDD.collect().foreach(println)
  }
}

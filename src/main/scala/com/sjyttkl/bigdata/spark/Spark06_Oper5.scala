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
object Spark06_Oper5 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    var sc: SparkContext = new SparkContext(config)

    var listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

    //将一个分区的数据，放到数组中
    var glomRDD: RDD[Array[Int]] = listRDD.glom()

    /*
    1,2
    3,4,5
    6,7,8
     */
    glomRDD.collect().foreach(array => {
      println(array.mkString(","))
    })
  }
}

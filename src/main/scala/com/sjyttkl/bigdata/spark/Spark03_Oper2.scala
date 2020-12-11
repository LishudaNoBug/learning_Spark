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
object Spark03_Oper2 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    var sc: SparkContext = new SparkContext(config)

    var listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    var mapPartitions: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(datas => datas * 2) //这里是 scala计算，不是RDD计算，会把这个整个发送给执行器exceuter
    })

    mapPartitions.collect().foreach(println)

  }
}

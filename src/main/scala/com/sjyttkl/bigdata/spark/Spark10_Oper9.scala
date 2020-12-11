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
object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    var sc: SparkContext = new SparkContext(config)

    var listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 2, 23, 3, 3, 4, 4, 4, 4, 6, 7, 7, 0))

    /*
    不传参数用默认分区数，传参数那么参数几就会有几个结果分区
     */
    var distinctRDD: RDD[Int] = listRDD.distinct(2)

    /*
    4     23
    1    3
    7    0
    6    2
     */
    distinctRDD.saveAsTextFile("output")
  }
}

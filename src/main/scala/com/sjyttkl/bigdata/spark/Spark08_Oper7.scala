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
object Spark08_Oper7 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    var sc: SparkContext = new SparkContext(config)

    var listRDD: RDD[Int] = sc.makeRDD(1 to 16)
    /*
    x => x % 2  :第一个x是listRDD中每一个元素作为参数，第二个x%2是函数体。
    将listRDD中所有元素能够%2==0为true的元素返回，为false的丢掉
    filterRDD里面放的就是所有%2为0的元素
     */
    var filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)
    filterRDD.collect().foreach(println)
  }
}

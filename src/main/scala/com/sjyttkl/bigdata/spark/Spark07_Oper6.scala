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
object Spark07_Oper6 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    var sc: SparkContext = new SparkContext(config)
    //分组后的数据，形成对偶元组（k-v),k表示分组的key  , value 表示分组的集合
    var listRDD: RDD[Int] = sc.makeRDD(1 to 16)

    /*
    i是listRdd的每个元素
    listRDD.groupBy(func):会根据func计算结果作为key分组，即i => i % 2为0的将0作key分一组；i => i % 2为1的将1作key分一组
    返回的groupbyRDD是k-v类型。key是0,1；v是计算结果为0,1的listRDD原数据
     */
    var groupbyRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

    groupbyRDD.collect().foreach(println)
  }
}

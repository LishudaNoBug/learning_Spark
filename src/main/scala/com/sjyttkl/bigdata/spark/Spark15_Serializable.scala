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
object Spark15_Serializable {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val sc = new SparkContext(config)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "songdongodng"))

    //da:这行是在Driver端执行
    val search = new Search("h")

    /*
    问题：Task not serializable
    分析:问题在于这行的getMatch1方法调用.filter()算子，算子中的逻辑都是在Execute执行
    所以filter算子中的isMatch逻辑需要发往Execute执行，但isMatch是成员方法需要将search对象
    整个也发给Execute，但此时search对象又是在Driver端创建的，所以需要将search对象序列化后发给Execute
    解决方法：类上extends  java.io.Serializable即可
     */
    val match1: RDD[String] = search.getMatch1(rdd)
    //    val match1:RDD[String] = search.getMatch2(rdd)
    match1.collect().foreach(println)
    sc.stop()
  }

}

class Search(query: String) extends java.io.Serializable {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  /**
    *
    * @param rdd rdd: RDD[String]——意思参数是个RDD，这个RDD内部是String类型
    * @return : RDD[String]——意思返回值是个RDD，RDD内部是String类型
    */
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //val 天生就能序列化，所以q直接就能传给Execute
    val q = query
    rdd.filter(x => x.contains(q))
  }
}
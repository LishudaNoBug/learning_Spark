package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/22 11:34
  * version: 1.0
  * description: spark_sql
  */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建SparkSession
    //必须以SparkSession.builder().config(config).getOrCreate()这种方式创建SparkSession同时加载配置文件
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //以session.read.json()方式创建DataFrame
    var frame: DataFrame = session.read.json("in/user.json")

    frame.show()

    session.stop()
  }
}

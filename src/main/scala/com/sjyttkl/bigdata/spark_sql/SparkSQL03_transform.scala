package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/22 11:34
  * version: 1.0
  * description: spark_sql
  */
object SparkSQL03_transform {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark_session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //SparkCore是通过sc这个上下文对象来创建RDD的，而SparkSession中内置了上下文对象所以可以通过内置的来创建RDD
    var rdd: RDD[(Int, String)] = spark_session.sparkContext.makeRDD(List((1, "zhagnshan"), (2, "宋冬冬")))
    //进行转换前，需要引入隐式转换规则。这里spark_session不是包的名字，是SparkSession的对象
    import spark_session.implicits._
    //转换为DF
    var df: DataFrame = rdd.toDF("id", "name")
    //转换为DS
    var ds: Dataset[User] = df.as[User] //这里需要创建样例类
    //转换为DF
    var df1: DataFrame = ds.toDF()
    //转换为RDD
    var rdd1: RDD[Row] = df1.rdd //这里是 row 类型需要注意

    rdd1.foreach(row => {
      //获取数据时，可以通过索引访问数据。（索引从0开始，列序号从1开始）
      println(row.getString(1))
    })

    spark_session.stop()
  }
}

//创建样例类，DF==》DS需要用到
case class User(id: Int, name: String);

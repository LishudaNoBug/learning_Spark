package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/22 11:34
  * version: 1.0
  * description: spark_sql
  */
object SparkSQL04_UDAF {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark_session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //创建RDD
    var rdd: RDD[(Int, String)] = spark_session.sparkContext.makeRDD(List((1, "zhagnshan"), (2, "宋冬冬")))

    //进行转换前，需要引入隐式转换规则。
    import spark_session.implicits._

    //用户自定义聚合函数
    //1. 创建聚合函数对象
    val udf = new MyAgeAvgFunction
    //2. 注册聚合函数
    spark_session.udf.register("avgAge", udf);
    //3. 使用聚合函数
    var frame: DataFrame = spark_session.read.json("in/user.json")
    frame.createOrReplaceTempView("user")
    spark_session.sql("select avgAge(age) from user").show

    spark_session.stop()
  }
}


//声明用户自定义聚合函数（弱类型）
// 继承：UserDefinedAggregateFunction，实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  //这个记住就好
  override def deterministic: Boolean = true

  //sql输入的数据及类型：即avgAge(age)的age是Long类型的。这里不能Long用LongType
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //sql最终输出的数据类型
  override def dataType: DataType = DoubleType


  //sql处理过程中用到的数据及类型：即avgAge过程中用到sum：long类型，count：long类型
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //初始化sql处理过程中用到的数据，只能通过buffer(index)来初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }


  //每个partition分区内部的计算逻辑
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //每个分区各自计算完之后，所有分区汇集到Driver节点。此时的汇总逻辑。
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }


  //最后一遍处理的计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}

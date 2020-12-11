package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql._

/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/23 11:34
  * version: 1.0
  * description: spark_sql
  */
object SparkSQL04_UDAF_Class {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark_session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import spark_session.implicits._

    //用户自定义聚合函数
    //1. 创建聚合函数对象
    val udf = new MyAgeAvgClassFunction

    //2. 将聚合函数转换TypedColumn
    var avgCol: TypedColumn[UserBean, Double] = udf.toColumn.name("avgAge")
    //3.在DS上执行select(sql函数)
    var frame: DataFrame = spark_session.read.json("in/user.json")
    var userDS: Dataset[UserBean] = frame.as[UserBean]
    userDS.select(avgCol).show()

    spark_session.stop()
  }
}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

//强类型实现自定义聚合函数
// 继承：Aggregator,设定泛型Aggregator[in,buffer,out]。  in指传入参数类型，buffer指中间运算的缓冲区的类类型，out指输出类型
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  //buffer类类型的初始化  sum=0;count=0
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //各partition分区内的聚合操作
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //各分区计算完返回给Driver节点的集合操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //最后一步求平均或求和的计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count.toDouble
  }

  //设置buffer和输出类型的编码，一般固定这么写
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

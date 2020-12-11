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
object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    var sc: SparkContext = new SparkContext(config)
    var listRDD: RDD[Int] = sc.makeRDD(1 to 6, 2)

    /*
        当有多个参数的复杂函数，先用一个大括号括起来启用模式匹配{     }
        num 是分区号，
        case（）里面的是参数   =>{    }里面的是计算逻辑
     */
    var tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号：" + num))
      }
    }
    tupleRDD.collect().foreach(println)
  }
}

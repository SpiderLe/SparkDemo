package com.king.yl.day04.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AccuTest {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("accu")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

    //    创建自定义累加器
    //    val myAccu = new MyAccu
    val mapAccu = new MapAccu

    //    注册累加器
    //    sc.register(myAccu)
    sc.register(mapAccu)

    rdd.foreach(x => {
      mapAccu.add(x)
      println(x)
    })
    println("************************" + mapAccu.value + "************************")
    sc.stop()

  }

}

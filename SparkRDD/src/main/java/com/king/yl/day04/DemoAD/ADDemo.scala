package com.king.yl.day04.DemoAD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ADDemo {

  def main(args: Array[String]): Unit = {

    //    1.初始化Spark配置
    val conf: SparkConf = new SparkConf().setAppName("adDemo").setMaster("local[*]")

    //    2.建立Spark连接
    val sc = new SparkContext(conf)

    //    3.从文件中读取数据，创建RDD对象（TS， Province, city, User, AD）
    val line: RDD[String] = sc.textFile("H:\\agent.log")

    //    4.按最小粒度聚合，省份和广告  ((pro,AD),1)
    val provinceAD2Count: RDD[((String, String), Int)] = line.map { x => {
      val items: Array[String] = x.split(" ")
      ((items(1), items(4)), 1)
    }
    }

    //    5.计算每个省份中每个广告被点击的总次数 ((pro,AD), count)
    val provinceAD2Sum: RDD[((String, String), Int)] = provinceAD2Count.reduceByKey(_ + _)

    //    6.扩大粒度范围  (pro, (AD, count))
    val pro2ADSum: RDD[(String, (String, Int))] = provinceAD2Sum.map(x => {
      (x._1._1, (x._1._2, x._2))
    })

    //    7.按照省份进行分组
    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = pro2ADSum.groupByKey()

    //    8.排序
    val result: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues(x => {
      x.toList.sortWith((x, y) => {
        x._2 > y._2
      }).take(3)
    })

    //    9.打印结果
    result.collect.foreach(println)

    //    10.关闭连接
    sc.stop()
  }
}


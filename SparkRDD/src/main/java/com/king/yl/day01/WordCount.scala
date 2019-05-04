package com.king.yl.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

List

      //1.创建SparkConf并设置App名称

      val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

      //2.创建SparkContext，该对象是提交Spark App的入口
      val sc = new SparkContext(conf)

      //3.使用sc创建RDD并执行相应的transformation和action
      val line: RDD[String] = sc.textFile("D:\\input\\input.txt")
      val word: RDD[String] = line.flatMap(_.split(" "))    //split("\\W")
      val wordandone: RDD[(String, Int)] = word.map((_, 1))
      val wordandcount: RDD[(String, Int)] = wordandone.reduceByKey(_ + _)

      wordandcount.saveAsTextFile("d:\\output")

      //4.关闭连接
      sc.stop()
    }



}

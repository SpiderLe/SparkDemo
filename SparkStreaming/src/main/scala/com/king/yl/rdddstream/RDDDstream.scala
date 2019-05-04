package com.king.yl.rdddstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object RDDDstream {

  def main(args: Array[String]): Unit = {

    //    1.初始化配置信息SparkConf
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdddstream")

    //    2.初始化SparkStreaming
    val ssc = new StreamingContext(sc, Seconds(2))
    //    日志打印级别
    ssc.sparkContext.setLogLevel("ERROR")

    //    3.创建RDD队列
    var rdd = new mutable.Queue[RDD[Int]]()

    //    4.创建Dstream
    val dstream: InputDStream[Int] = ssc.queueStream(rdd, oneAtATime = true)

    val mapstream: DStream[(Int, Int)] = dstream.map((_, 1))
    val redstream: DStream[(Int, Int)] = mapstream.reduceByKey(_ + _)

    //    5.打印结果
    redstream.print()

    //    6.启动任务
    ssc.start()
    //    7.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rdd += ssc.sparkContext.makeRDD(1 to 10, 2)
      Thread.sleep(1000)
    }
    ssc.awaitTermination()
  }

}

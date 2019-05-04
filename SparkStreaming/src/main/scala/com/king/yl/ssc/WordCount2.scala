package com.king.yl.ssc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCount2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, streaming.Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop101",9999)

    val dstream: DStream[(String, Int)] = line.transform(rdd => {
      val word: RDD[String] = rdd.flatMap(_.split(" "))
      val wordAndOne: RDD[(String, Int)] = word.map((_, 1))
      val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

      wordAndCount
    })

    dstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

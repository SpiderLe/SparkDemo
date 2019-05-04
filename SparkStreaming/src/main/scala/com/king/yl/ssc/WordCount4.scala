package com.king.yl.ssc

import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCount4 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, streaming.Seconds(3))
//    ssc.sparkContext.setLogLevel("ERROR")

    ssc.checkpoint("./ck4")
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop101",9999)

    val word: DStream[String] = line.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = word.map(x=>(x,1))

    //加上新进入窗口的批次中的元素 //移除离开窗口的老批次中的元素 //窗口时长// 滑动步长 //分区 // filterFunc
    val wordAndCount: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow(
      (x:Int,y:Int)=>(x+y),
      (x:Int,y:Int)=>(x-y),
      Seconds(12),
      Seconds(3),
      4,
      (x:(String, Int)) => x._2 > 0)
    wordAndCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

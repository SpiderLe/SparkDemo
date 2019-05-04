package com.king.yl.ssc

import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, streaming.Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop101",9999)

    val word: DStream[String] = line.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = word.map(x=>(x,1))

    val wordCount: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()




  }

}

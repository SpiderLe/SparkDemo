package com.king.yl.ssc

import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.streaming.state
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCount3 {


  val updateFunc = (values : Seq[Int], state : Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, streaming.Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("./ck3")

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop101", 9999)

    val word: DStream[String] = line.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = word.map(x => (x, 1))

    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val wordAndOneDsatream: DStream[(String, Int)] = wordAndOne.updateStateByKey[Int](updateFunc)

    wordAndOneDsatream.print()

    ssc.start()
    ssc.awaitTermination()


  }


}

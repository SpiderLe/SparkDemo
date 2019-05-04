package com.king.yl.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HighKafka2 {

  def createSSC(): StreamingContext = {
    //    1.创建SparkConf
    val sc: SparkConf = new SparkConf().setAppName("highKafka2").setMaster("local[*]")

    //    2.创建SparkStreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))

    //    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("./ck2")

    //    3.Kafka参数声明
    val brokers = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    val topic = "hi"
    val group = "bigdata"
    //    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //    4.封装Kafka参数
    val kafkaParams: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group
      //      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      //      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    //    5.读取Kafka数据创建Dstream
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,
      String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("hi"))

    //    6.打印结果
    //    kafkaStream.print()
    val wordStreams: DStream[String] = kafkaStream.flatMap(_._2.split("\\W"))
    val result: DStream[(String, Int)] = wordStreams.map((_, 1)).reduceByKey(_ + _)
    result.print()
    ssc
  }

  def main(args: Array[String]): Unit = {

    //   获取SSC
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./l", createSSC)
    ssc.sparkContext.setLogLevel("ERROR")

    ssc.start()
    ssc.awaitTermination()
  }
}

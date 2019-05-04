package com.king.yl.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, dstream}

import scala.collection.immutable.HashMap


object LowerKafka {

  def getOffsets(kafkaCluster: KafkaCluster, group: String, topic: String): Map[TopicAndPartition, Long] = {

    //    定义一个最终返回值 ： 主题和分区 -> offset
    var partitionToLong = new HashMap[TopicAndPartition, Long]()

    //    根据topic获取分区
    val topicAndPartitionsEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //    判断分区信息是否为空
    if (topicAndPartitionsEither.isRight) {

      //   分区信息不为空，取出分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionsEither.right.get

      //    获取消费者消费数据的数据
      val topicAndPartitionToLongEither: Either[Err, Map[TopicAndPartition, Long]] =
        kafkaCluster.getConsumerOffsets(group, topicAndPartitions)

      //      判断offset是否存在
      if (topicAndPartitionToLongEither.isLeft) {
        //      不存在，即未消费过，遍历每一个分区
        // 存在问题：之前消费过，数据已清除
        for (topicAndPartition <- topicAndPartitions) {
          partitionToLong += (topicAndPartition -> 0L)
        }
      } else {
        //        offset存在，之前消费过,取出offset
        val value: Map[TopicAndPartition, Long] = topicAndPartitionToLongEither.right.get

        partitionToLong ++= value
      }
    }
    partitionToLong
  }

  def saveOffset(kafkaCluster: KafkaCluster, kafkaDstream: InputDStream[String], group: String) = {

    kafkaDstream.foreachRDD(rdd => {

      var partitionToLong = new HashMap[TopicAndPartition, Long]()

      //      取出rdd中的offset
      val offsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      //      获取所有分区的offsetRanges
      val ranges: Array[OffsetRange] = offsetRanges.offsetRanges

      //      保存每个分区的消费信息
      for (range <- ranges) {
        val offset: Long = range.untilOffset
        partitionToLong += range.topicAndPartition() -> offset
      }

      //    保存消费offset
      kafkaCluster.setConsumerOffsets(group, partitionToLong)
    })
  }

  def main(args: Array[String]): Unit = {

    //    1.创建SparkConf
    val sc: SparkConf = new SparkConf().setAppName("highKafka").setMaster("local[*]")

    //    2.创建SparkStreamingContext
    val ssc = new StreamingContext(sc, Seconds(3))

    ssc.sparkContext.setLogLevel("ERROR")
    //    3.Kafka参数声明
    val brokers = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    val topic = "spider"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //    4.封装Kafka参数
    val kafkaParams: Map[String, String] = Map(
      "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    val kafkaCluster = new KafkaCluster(kafkaParams)
    //offset默认存储在zookeeper
    val fromOffsets: Map[TopicAndPartition, Long] = getOffsets(kafkaCluster, group, topic)

    //    5.读取Kafka数据创建Dstream
    val kafkaDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc
      , kafkaParams
      , fromOffsets
      , (message: MessageAndMetadata[String, String]) => message.message())

    kafkaDstream.print

    //  保存新的offset
    saveOffset(kafkaCluster, kafkaDstream, group)

    ssc.start()
    ssc.awaitTermination()
  }
}

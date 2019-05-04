package com.king.yl.day04.accumulator

import org.apache.spark.util.AccumulatorV2

class MapAccu extends AccumulatorV2[Int, Map[String, Double]] {

  //  var map = ("sum"-> 0d, "avg" -> 0d, "count" -> 0d)

  var map: Map[String, Double] = Map[String, Double]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
    val mapAccu: MapAccu = new MapAccu
    mapAccu.map ++= map
    mapAccu
  }

  override def reset(): Unit = {
    map = Map[String, Double]()
  }

  override def add(v: Int): Unit = {
    map += "sum" -> (map.getOrElse("sum", 0d) + v)
    map += "count" -> (map.getOrElse("count", 0d) + 1)
  }

  override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
    other match {
      case o: MapAccu =>
        map += "sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
        map += "count" -> (map.getOrElse("count", 0d) + o.map.getOrElse("count", 0d))
      case _ =>
    }
  }

  override def value: Map[String, Double] = {

    map += ("avg" -> map.getOrElse("sum", 0d) / map.getOrElse("count", 0d))
    map
  }
}

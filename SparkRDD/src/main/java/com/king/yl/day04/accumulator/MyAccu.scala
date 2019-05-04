package com.king.yl.day04.accumulator

import org.apache.spark.util.AccumulatorV2

class MyAccu extends AccumulatorV2[Int, Int] {

  var sum: Int = 0

  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[Int, Int] = {

    val acc: MyAccu = new MyAccu
    acc.sum = this.sum
    acc
  }

  override def reset(): Unit = {
    sum = 0
  }

  override def add(v: Int): Unit = sum += v

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {

    sum += other.value

  }

  override def value: Int = sum
}

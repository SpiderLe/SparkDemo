package com.king.yl.sparkSqlHive

import org.apache.spark.sql.SparkSession

object Spark2HiveDemo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("spark2hive")
      .getOrCreate()

    spark.sql("show tables").show()
//    spark.sql("create table testspark(id int)")

    spark.stop()
  }

}

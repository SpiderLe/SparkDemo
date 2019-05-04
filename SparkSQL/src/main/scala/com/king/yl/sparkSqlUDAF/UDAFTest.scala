package com.king.yl.sparkSqlUDAF

import org.apache.spark.sql.{DataFrame, SparkSession}

object UDAFTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("udafDemo")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("D:\\SparkDemo\\SparkSQL\\src\\people.json")
    df.show()
    df.createTempView("people")

    spark.udf.register("avg", new UDAFDemo)

    val result: DataFrame = spark.sql("select avg(age) as avg_age from people")
    result.show()
    println("*******************************")
    spark.stop()
  }

}

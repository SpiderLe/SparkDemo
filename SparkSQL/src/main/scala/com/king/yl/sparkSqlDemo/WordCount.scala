package com.king.yl.sparkSqlDemo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount {

  case class People (name:String, age:Int)

  def main(args: Array[String]): Unit = {
    //  创建SparkSession
    val spark: SparkSession =
      SparkSession.builder()
        .appName("wordcount")
        .master("local[*]")
        .getOrCreate()

    //  导入隐式转换
    import spark.implicits._

    //  读取文件
    val df: DataFrame = spark.read.json("D:\\SparkDemo\\SparkSQL\\src\\people.json")

    df.show()


//    DSL风格
    df.select("name").show()

    println("***************************************")


//    Sql风格
    df.createTempView("people")
    spark.sql("select * from people where age > 20").show()

//    df-->ds
    val ds: Dataset[People] = df.as[People]
    ds.show()
    println("**************ds.show**************")

    spark.stop()
  }

}

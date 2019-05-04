package com.king.yl.day04.mysqlRDD



import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {

  def main(args: Array[String]): Unit = {


    // 1.创建spark配置信息
    val conf: SparkConf = new SparkConf().setAppName("MYSQLRDD").setMaster("local[*]")

    //2.创建sparkContext
    val sc = new SparkContext(conf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop101:3306/company"
    val userName = "root"
    val passWd = "000000"

    //4.创建JdbcRDD
    val result = new JdbcRDD[Int](sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `staff` where `id`>=? and `id`<=?;",
      1,
      3,
      1,
      (x: ResultSet) => x.getInt(1)
    )

    //5.打印结果
    println(result.count())

    result.foreach(println)

    sc.stop()


  }

}

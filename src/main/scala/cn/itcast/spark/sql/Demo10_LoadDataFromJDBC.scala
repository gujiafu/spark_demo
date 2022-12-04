package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:49
 * Desc: 通过JDBC从外部数据库中加载数据
 */
object Demo10_LoadDataFromJDBC {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
     * 读取有3个方式:
     * 1. 单分区
     * 2. 多分区
     * 3. 自由分区
     */
    // 先准备基本对象
    val url = """jdbc:mysql://192.168.88.163:3306/test?characterEncoding=utf8&useUnicode=true"""
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("fetchsize", "100")
    properties.setProperty("queryTimeout", "30")
    // 单分区模式
    val df1: DataFrame = spark.read
      .jdbc(url, "person", properties)
    df1.printSchema(); df1.show()

    // 多分区模式
    /**
     * low 1 upp 10 partitions 5
     * where id < 1
     * where id >=1 and id <=3
     * where id>3 and id <=6
     * where id>6 and id <=10
     * where id > 10
     *
     * 一定要注意, lowerBound和upperBound不管怎么设置, 都会查询表[[[全部的数据]]]
     */
    val df2: DataFrame = spark.read
      .jdbc(
        url,
        "person",
        "id",
        1,
        5,
        3,
        properties
      )

    df2.show()


    // 自由分区模式
    val df3: DataFrame = spark.read
      .jdbc(
        url,
        "person",
        Array(
          // 想要几个分区就写几个. 要注意.条件是自己控制的. 控制的有遗漏, 那么就会遗漏数据, 或者条件重复了就会重复数据.
          // 数据倾斜不要怪Spark, 因为分区条件自己给的.
          "id <= 5", // 分区1的条件
          "id > 5 AND id <= 7", // 分区2的条件
          "id > 7" // 分区3的条件
        ),
        properties
      )

    df3.show()


    /**
     * 写数据到JDBC(炒鸡简单)
     */

    df3.write
      // 表不存在 会自动的创建滴.
      .mode(SaveMode.Overwrite)
      .jdbc(url, "person2", properties)





    sc.stop()
  }
}

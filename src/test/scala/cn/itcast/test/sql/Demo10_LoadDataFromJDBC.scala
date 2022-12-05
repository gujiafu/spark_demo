package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql._

import java.util.Properties

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:49
 * Desc: 通过JDBC从外部数据库中加载数据
 */
object Demo10_LoadDataFromJDBC {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val url: String = """jdbc:mysql://192.168.88.163:3306/test?characterEncoding=utf8&useUnicode=true"""
    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("fetchsize", "100")
    properties.setProperty("queryTimeout", "100")

    // jdbc单分区查询
    val df: DataFrame = spark.read.jdbc(url, "person", properties)
    df.printSchema()
    df.show()

    // jdbc多分区查询
    spark.read.jdbc(
      url,
      "person",
      "id",
      5,
      7,
      3,
      properties)
      .show()

    // jdbc自由分区查询
    spark.read.jdbc(
      url,
      "person",
      Array(
        "id <=5",
        "id>5 and id <=7",
        "id >7"),
      properties)
      .show()

    // jdbc 插入
    df.write.mode(SaveMode.Overwrite).jdbc(url, "person2", properties)

    sc.stop()
  }
}

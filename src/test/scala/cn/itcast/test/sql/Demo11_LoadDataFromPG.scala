package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:49
 * Desc: 通过JDBC从外部数据库中加载数据
 */
object Demo11_LoadDataFromPG {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val goodDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://192.168.88.166:5432/flinkx?characterEncoding=utf8&useUnicode=true")
      .option("user", "flinkx")
      .option("password", "flinkx")
      .option("query", "select * from bigdata.good where id >1")
      .load()
    goodDF.show(1000, true)

    val saleDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://192.168.88.166:5432/flinkx?characterEncoding=utf8&useUnicode=true")
      .option("user", "flinkx")
      .option("password", "flinkx")
      .option("query", "select * from bigdata.sale where good_id >1")
      .load()
    saleDF.show(1000, true)

    goodDF.createOrReplaceTempView("good")
    saleDF.createOrReplaceTempView("sale")

    val saleDetailDF: DataFrame = spark.sql(
      """
        |SELECT
        |	s.id,
        |	g.id as good_id,
        |	g.name AS good_name,
        |	g.price AS good_price,
        |	s.num,
        |	s.num * g.price AS total_money,
        |	s.time
        |FROM
        |	sale s
        |LEFT JOIN
        |	good g
        |ON s.good_id = g.id
        |order by id
        |""".stripMargin)

    saleDetailDF.show(true)

    saleDetailDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://192.168.88.166:5432/flinkx?characterEncoding=utf8&useUnicode=true")
      .option("user", "flinkx")
      .option("password", "flinkx")
      .option("dbtable", "bigdata.sale_detail")
      .mode(SaveMode.Append)
      .save()

    sc.stop()
  }
}

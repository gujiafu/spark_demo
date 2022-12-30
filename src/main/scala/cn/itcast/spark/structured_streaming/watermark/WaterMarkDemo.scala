package cn.itcast.spark.structured_streaming.watermark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

/**
 * Author: itcast caoyu
 * Date: 2021-04-17 0017 21:12
 * Desc: 演示Spark StructuredStreaming 中的 WaterMark和最大乱序时间概念
 * WaterMark 称之为: 水印\水位线
 */
object WaterMarkDemo {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      // 测试阶段经常用这个 参数
      // 参数用来设置: 在shuffle阶段 默认的分区数量(如果你不设置默认是200)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 经常用到的两个隐式转换我们也写上
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Source
    val socketDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node3")
      .option("port", 9999)
      .load()

    val resultDF: DataFrame = socketDF
      .filter(row => StringUtils.isNotBlank(row.getString(0)))
      .map(row => {
        val data = row.getString(0)
        val strDate: String = data.split(",")(0)
        val word: String = data.split(",")(1)

        Timestamp.valueOf(strDate) -> word
      }).toDF("timestamp", "word")
      // 进行处理, 我们要加WaterMark
      // 要用事件时间, 必须带WaterMark, 也就是这个withWaterMark的方法
      // 也就告诉了spark, 按照事件时间计算.
      // 参数1: 告知spark 事件时间在哪个列. 参数2: 最大允许乱序时间
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        // 设置窗口
        window($"timestamp", "10 seconds"),
        // 指定分组列
        $"word")
      .count()

    resultDF.writeStream
      .outputMode("complete")
      .option("truncate", value = false)
      .format("console")
      // Trigger(微批) 只支持 处理时间.
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

    sc.stop()
  }
}
// chuangkou : 取一节数据, 计算他们的结果.
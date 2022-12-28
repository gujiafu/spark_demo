package cn.itcast.test.structured_streaming.watermark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Timestamp

/**
 * Author: itcast caoyu
 * Date: 2021-04-17 0017 21:12
 * Desc: 演示Spark StructuredStreaming 中的 WaterMark和最大乱序时间概念
 * WaterMark 称之为: 水印\水位线
 */
object WaterMarkDemo {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 输入
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 转换
    val resultDF: DataFrame = df.as[String]
      .filter(line => StringUtils.isNoneEmpty(line))
      .map(line => {
        val timeStamp: String = line.split(",")(0)
        val word: String = line.split(",")(1)
        Timestamp.valueOf(timeStamp) -> word
      })
      .toDF("timeStamp", "word")
      .withWatermark("timeStamp", "10 seconds")
      .groupBy(
        window($"timeStamp", "10 seconds"),
        $"word")
      .count()

    // 输出
    resultDF.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode(OutputMode.Complete())
      .start()

    spark.streams.awaitAnyTermination()

    sc.stop()
  }
}

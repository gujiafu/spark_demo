package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.UUID

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 21:05
 * Desc: 演示StructuredStreaming的File Sink
 */
object Demo08_FileSInk {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val socketDF: DataFrame = spark.readStream
      .format("socket")   // socket 不支持容错
      .option("host", "node3")
      .option("port", 9999)
      .load()

    // 简单处理, 因为FileSink 只支持Append模式, 也就是不能写聚合, 那就是
    // 无状态计算, 也就是简单过滤过滤
    // 过滤数据, 不要hello
    val resultDF: Dataset[Row] = socketDF.filter(row => !row.getString(0).equals("hello"))

    // 数据的输出
    val query: StreamingQuery = resultDF.writeStream
      .format("csv")
      .option("path", "data/output/csv")
      // 不要忘记CKP, 确保一致性
      .option("checkpointLocation", "data/ckp/" + UUID.randomUUID().toString)
      .outputMode(OutputMode.Append()) // File Sink 只支持 Append
      .start()

    query.awaitTermination()

    sc.stop()
  }
}

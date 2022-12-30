package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.UUID

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 21:05
 * Desc: 演示StructuredStreaming的ForeachBatch Sink
 */
object Demo09_ForeachBatchSInk {
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

    val socketDF: Dataset[String] = spark.readStream
      .format("socket")   // socket 不支持容错
      .option("host", "node3")
      .option("port", 9999)
      .load()
      .flatMap(row => row.getString(0).split(" "))

    val resultDF: DataFrame = socketDF.toDF("word")
      .groupBy($"word")
      .count()

    // 数据的输出
    val query: StreamingQuery = resultDF.writeStream
      .outputMode(OutputMode.Complete())
//      .option("checkpointLocation", "data/ckp/foreachbatch")
      // 一次处理一个批次的数据结果的方法
      // 要求你写一个函数, 函数2个形参, 形参1是DataSet[Row] 存储的是 一个批次的结果  形参2: 当前处理批次的ID(从0开始计算)
      .foreachBatch(
        (batchDS: Dataset[Row], batchID:Long) => {
          /* 要注意, 进入到foreachBatch中, 每一个批次的数据处理 都是批处理了, 不是流计算了 */
          // 进来后, 当前这个批次的数据就拿到了.  拿到后, 想怎么处理就怎么处理
          // 打印一下批次号
          println("-------------------")
          println(s"=== BatchID: ${batchID}")
          println("-------------------")

          // 处理批次数据
          // 先show一下
          batchDS.show()
          // 写出到mysql
          batchDS.write
            .option("url", "jdbc:mysql://node3:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "test.ss_wordcount_output")
            .format("jdbc")
            .mode(SaveMode.Append)
            .save()
        }
      ).start()

    query.awaitTermination()

    sc.stop()
  }
}

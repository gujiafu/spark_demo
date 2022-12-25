package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 20:20
 * Desc: 入门案例:Socket 数据源
 */
object Demo01_SocketSource {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      // 测试阶段经常用这个 参数
      // 参数用来设置: 在shuffle阶段 默认的分区数量(如果你不设置默认是200)
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 经常用到的两个隐式转换我们也写上
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Source - Socket
    // 返回值还是DataFrame, 但是这个DataFrame就会有Spark自动给其追加新数据
    // 让其成为无界的DF
    val df: DataFrame = spark.readStream
      .format("socket")   // socket 不支持容错
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val result: DataFrame = df.groupBy("value")
      .count()

    // 输出看看
    result.writeStream
      .outputMode("complete")
      .format("console")  // 表示输出到控制台
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()   // 阻塞

    sc.stop()
  }
}

/**
 Structured Streaming 自动做状态累加
 无需手动写 什么:updateStateByKey 或者 mapWithState

 用的不是:updateStateByKey 或者 mapWithState

 基于无界DF保存的
 */
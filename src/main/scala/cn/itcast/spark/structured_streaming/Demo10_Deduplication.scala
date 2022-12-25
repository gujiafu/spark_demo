package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 21:54
 * Desc: 演示Spark的 全局去重算子
 * {"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:55","eventType": "browse","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:55","eventType": "click","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "aaa","userID":"1"}
 * {"eventTime": "2016-01-10 10:02:00","eventType": "click","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:51","eventType": "click","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
 * {"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"3"}
 * {"eventTime": "2016-01-10 10:01:51","eventType": "aaa","userID":"2"}
 */
object Demo10_Deduplication {
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

    val socketDF: DataFrame = spark.readStream
      .format("socket") // socket 不支持容错
      .option("host", "node3")
      .option("port", 9999)
      .load()

    // DF 转 DS  用as方法  对泛型做替换
    val resultDF: DataFrame = socketDF.as[String]
      // 传入数据是json, 我们需要用sql中的get_json_object函数来获取json中的字段
      .select(
        // 默认的列名 叫做value
        // 取json的列, $表示的是json的root(根)
        get_json_object($"value", "$.eventTime").as("event_time"),
        get_json_object($"value", "$.eventType").as("event_type"),
        get_json_object($"value", "$.userID").as("user_id")
        // 通过这个selec + get_json_object方法就将数据转变成了 一个3个列的df对象
      )
      .dropDuplicates("user_id", "event_type")
      .groupBy("user_id")
      .count()

    resultDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

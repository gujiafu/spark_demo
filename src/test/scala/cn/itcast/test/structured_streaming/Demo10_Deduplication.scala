package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
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
 * {"eventTime": "2016-01-10 10:01:51","eventType": "bbb","userID":"2"}
 */
object Demo10_Deduplication {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // source
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 999)
      .load()

    // transform
    val resultDF: DataFrame = df.as[String].select(
      get_json_object($"value", "$.eventTime").as("eventTime"),
      get_json_object($"value", "$.eventType").as("eventType"),
      get_json_object($"value", "$.userID").as("userID")
    ).dropDuplicates("eventType", "userID")
      .groupBy("userID")
      .count()

    // sink
    resultDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

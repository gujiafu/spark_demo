package cn.itcast.spark.structured_streaming.kafka

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-17 0017 15:07
 * Desc: StructuredStreaming整合Kafka案例
 * +------------------------------------------------------------+
 * |value                                                       |
 * +------------------------------------------------------------+
 * |station_6,18600002699,18900004912,success,1618645571297,4000|
 * |station_0,18600008193,18900004960,success,1618645571435,5000|
 * |station_5,18600007814,18900005173,busy,1618645571155,0      |
 * +------------------------------------------------------------+
 */
object StructuredStreamingWithKafka {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      // 必填属性
      .option("kafka.bootstrap.servers", "node1:9092, node2:9092, node3:9092")
      .option("subscribe", "raw-topic")
      // 指定消费从哪开始
      .option("startingOffsets", """{"raw-topic":{"2":291,"1":290,"0":290}""")
//      .option("startingOffsets", "latest")
      // 数据丢失是否失败, 报异常
      .option("failOnDataLoss", true)
      // 超时时间毫秒
      .option("kafkaConsumer.pollTimeoutMs", 60000L)
      // 重试次数
      .option("fetchOffset.numRetries", 3)
      // 每次重试间隔时间 毫秒
      .option("fetchOffset.retryIntervalMs", 10L)
      //      .option("maxOffsetsPerTrigger", )
      //      .option("minPartitions")
      .load()

    // 处理数据, 只保留success标记的数据
    val resultDF: Dataset[Row] = kafkaDF.selectExpr("CAST(value AS STRING)")
      .filter((row: Row) => {
        val data: String = row.getString(0)
        // 执行过滤, 要求:1. 正常数据 不是混乱数据, 2. 标记为success才要
        StringUtils.isNoneBlank(data) && "success".equals(data.trim.split(",")(3))
      })

//    // 打印试试
//    resultDF.writeStream
//      .format("console")
//      .outputMode(OutputMode.Append())
//      .option("truncate", false)
//      .start()
//      .awaitTermination()

    // 将处理过滤后的干净数据 写回 kafka的 etl-topic 中
    val resultDFWithTopicField: DataFrame =
      resultDF.map(row => row.getString(0) -> "etl-topic").toDF("value", "topic")
    resultDFWithTopicField.writeStream
      .format("kafka")
      // 只需要这两个option,而且也只有这2个option可以用.
      .option("kafka.bootstrap.servers", "node1:9092, node2:9092, node3:9092")
      /**
       * topic 这个属性是可选的.
       * 当写出的DataFrame中有topic这个列, 这个topic option可以不写
       * 如果你写出的DataFrame中没有topic这个列, 下面的topic option就必须写.
       */
//      .option("topic", "etl-topic")
      .outputMode(OutputMode.Append())
      // checkpoint 必须设置
      .option("checkpointLocation", "data/ckp")
      .start()
      .awaitTermination()

    sc.stop()
  }
}

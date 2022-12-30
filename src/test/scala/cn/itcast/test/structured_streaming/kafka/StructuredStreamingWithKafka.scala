package cn.itcast.test.structured_streaming.kafka

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "raw-topic")
      .option("startingOffsets", "latest")
//      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.printSchema()



    // transform
    val resultDF: Dataset[String] = kafkaDF
      .selectExpr("CAST(value as string)")
      .as[String]
      .filter(value => StringUtils.isNoneBlank(value) && value.trim.split(",")(3).equals("success"))

    // sink

    //    resultDF.writeStream
    //      .format("console")
    //      .outputMode(OutputMode.Append())
    //      .option("truncate", false)
    //      .start()
    //      .awaitTermination()

    resultDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("topic", "etl-topic")
      .option("checkpointLocation", "data/ckp/kafka")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()

    sc.stop
  }
}

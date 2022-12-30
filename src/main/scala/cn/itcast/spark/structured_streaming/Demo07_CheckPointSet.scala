package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 20:01
 * Desc: 演示StructuredStreaming中输出的检查点位置的设置(用来实现精确一致性)
 */
object Demo07_CheckPointSet {
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

    /**
     * 文件的Source 支持的是  添加新文件的方式
     * 不支持的是,对文件内容进行追加
     */
    val csvDF: DataFrame = spark.readStream
      .format("csv")
      .option("sep", ";")
      .schema("name STRING, age INT, hobby STRING")
      .load("data/input/persons")

    // 数据处理 WordCount
    csvDF.createOrReplaceTempView("csv")
    val resultDF: DataFrame = spark.sql(
      """
        |SELECT hobby, COUNT(*) AS cnt FROM csv GROUP BY hobby
        |""".stripMargin)

    // 数据输出
    val query: StreamingQuery = resultDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      // 设置检查点的路径
      .option("checkpointLocation", "data/ckp")
      .start()

    query.awaitTermination()

    sc.stop()
  }
}

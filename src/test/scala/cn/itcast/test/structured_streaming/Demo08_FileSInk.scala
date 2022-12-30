package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 21:05
 * Desc: 演示StructuredStreaming的File Sink
 */
object Demo08_FileSInk {
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
    val filterDF: Dataset[Row] = df.filter(row => !row.getString(0).equals("aaa"))

    // sink
    filterDF.writeStream
      .format("csv")
      .outputMode(OutputMode.Append())
      .option("path", "data/output/csv")
      .option("checkpointLocation", "data/ckp_csv")
      .start()
      .awaitTermination()

    sc.stop()
  }
}

package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 20:20
 * Desc: 入门案例:Socket 数据源
 */
object Demo01_SocketSource {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐性转换依赖
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // source
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // transform
    val resultDF: DataFrame = df.groupBy("value").count()

    // sink
    resultDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    sc.stop()
  }
}


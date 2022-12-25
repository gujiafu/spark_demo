package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 21:10
 * Desc: 演示StructuredStreaming的RateSource源
 * Rate源是模拟生成数据的, 一般是用来调通代码的
 * 生产不用.
 */
object Demo03_RateSource {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    // 经常用到的两个隐式转换我们也写上
    import spark.implicits._
    import org.apache.spark.sql.functions._


    // Rate Source
    val rateDF: DataFrame = spark.readStream
      .format("rate") // rate 数据源
      .option("rowsPerSecond", "10") // 每秒生成的数据条数
      .option("rampUpTime", "0s") // 生成数据的时间间隔
      .option("numPartitions", 2) // 生成的分区数量

      .load()
    rateDF.printSchema()

    /**
     * Rate数据源生成的数据结构就2个列
     * 1. 时间戳
     * 2. 叫做value, 内容是递增的long数字
     * 数据的内容无法控制, rate就是不停的产生数据而已
     */

    rateDF.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")  // 这个truncate适用于所有的源 用于字段过长进行省略 true表示省略 false表示不省略
      .start()
      .awaitTermination()

    // StructuredStreaming 默认无需设置微批间隔, spark会尽快处理一个批次的数据

    sc.stop()
  }
}

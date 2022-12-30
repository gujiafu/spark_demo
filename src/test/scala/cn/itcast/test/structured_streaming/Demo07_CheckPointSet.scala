package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 20:01
 * Desc: 演示StructuredStreaming中输出的检查点位置的设置(用来实现精确一致性)
 */
object Demo07_CheckPointSet {
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

    // source 读文件
    val df: DataFrame = spark.readStream
      .format("csv")
      .schema("name string, age int, hobby string")
      .option("sep", ";")
      .load("data/input/persons")

    // 转换
    df.createOrReplaceTempView("hobby_t")
    val resultDF: DataFrame = spark.sql(
      """
        |select hobby,count(*) as cnt from hobby_t group by hobby
        |""".stripMargin)

    // sink
    resultDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "data/ckp")
      .start()
      .awaitTermination()

    sc.stop()
  }
}

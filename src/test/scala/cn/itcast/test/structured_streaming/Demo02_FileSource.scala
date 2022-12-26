package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 20:57
 * Desc: 演示StructuredStreaming的File数据源
 */
object Demo02_FileSource {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // source 读文件
    val df: DataFrame = spark.readStream
      .format("csv")
      .option("sep", ";")
      .schema("name string, age int, hobby string")
      .load("data/input/persons")

    // sink 输出
    df.printSchema()

    df.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

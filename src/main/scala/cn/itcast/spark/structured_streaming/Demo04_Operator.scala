package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 21:21
 * Desc: 演示流计算的数据处理
 * 流计算的数据处理和批处理的SparkSQL数据处理没有任何API上的区别
 * StructuredStreaming 和 SparkSQL在API的不同点 只在于:
 * - input
 * - output
 * 两个地方
 * 在于数据处理方面是一模一样的.
 * 因为都是DataFrame
 */
object Demo04_Operator {
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


    // 数据输入 Socket
    val socketDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 处理数据, 以WordCount为例DataFrame == DataSet[Row]
    val wordDF: DataFrame = socketDF.flatMap(row => row.getString(0).split(" ")).toDF("word")

    /**
     * SQL 风格
     */
    wordDF.createOrReplaceTempView("words")
    val sqlResult: DataFrame = spark.sql(
      """
        |SELECT word, COUNT(*) AS cnt
        |FROM words
        |GROUP BY word

        |""".stripMargin)


    /**
     * DSL 风格
     */
    val dslResult: Dataset[Row] = wordDF.groupBy($"word")
      .count()
//      .orderBy($"count".desc)


    val q1: StreamingQuery = sqlResult.writeStream
      .format("console")
      .outputMode("update")
      .start()

    val q2: StreamingQuery = dslResult.writeStream
      .format("console")
      .outputMode("update")
      .start()

    spark.streams.awaitAnyTermination()

    sc.stop()
  }
}

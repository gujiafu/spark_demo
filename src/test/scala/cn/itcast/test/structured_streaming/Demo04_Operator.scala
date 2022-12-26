package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
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

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[3]")
      .config("spark.sql.shuffle.partitions", 9)
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
      .option("port", 9999)
      .load()

    // transform
    val wordDF: DataFrame = df.flatMap(row => row.getString(0).split(" ")).toDF("word")
    wordDF.createOrReplaceTempView("word_t")
    val resultDF: DataFrame = spark.sql(
      """
        |select word, count(*) as cnt from word_t
        | group by word
        | order by cnt desc
        |""".stripMargin)

//    val resultDF: Dataset[Row] = wordDF
//      .groupBy($"word")
//      .count()
//      .orderBy($"count".desc)
//      .where(""" word<>'a' """)

    // sink
    resultDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

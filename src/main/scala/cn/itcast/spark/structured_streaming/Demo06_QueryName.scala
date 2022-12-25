package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 20:01
 * Desc: 演示structured_streaming中的查询名称概念
 */
object Demo06_QueryName {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._


    // 数据源 Socket
    val socketDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node3")
      .option("port", 9999)
      .load()

    // 数据处理 WordCount
    val wordDF: DataFrame = socketDF.flatMap(row => row.getString(0).split(" ")).toDF("word")
    wordDF.createOrReplaceTempView("words")
    val resultDF: DataFrame = spark.sql(
      """
        |SELECT word, COUNT(*) AS cnt FROM words GROUP BY word
        |""".stripMargin)

    // 数据输出
    val query: StreamingQuery = resultDF.writeStream
      // 查询名称只能用内存输出
      .format("memory")
      .outputMode(OutputMode.Complete())
      // 给当前输出的结果 标记一个名称 ,叫做testQuery
      .queryName("testQuery")
      .start()

    /**
     * 给流的输出中设置查询名称的用途是:
     * 对这个结果集进行二次查询处理的用途
     * 注意:只能用memory输出 才支持queryName方法
     */
    while (true) {  // 类似于阻塞
      Thread.sleep(1000L)
      spark.sql("SELECT * FROM testQuery").show()
    }

    query.awaitTermination()

    sc.stop()
  }
}

package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 20:01
 * Desc: 演示structured_streaming中的查询名称概念
 */
object Demo06_QueryName {
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

    // 输入
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 转换
    val wordDF: DataFrame = df.flatMap(row => row.getString(0).split(" ")).toDF("word")
    wordDF.createOrReplaceTempView("word_t")
    val resultDF: DataFrame = spark.sql(
      """
        |select word, count(*) as cnt from word_t
        |group by word
        |""".stripMargin)

    // 输出
    resultDF.writeStream
      .format("memory")
      .outputMode(OutputMode.Complete())
      .queryName("tmp_word_t")
      .start()

    while (true){
      Thread.sleep(1000)
      spark.sql(
        """
          |select word, cnt * 10 from tmp_word_t
          |""".stripMargin).show()
    }

    sc.stop()
  }
}

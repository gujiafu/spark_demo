package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:26
 * Desc: 演示SparkSQL 执行MySQL8.0后支持的窗口函数
 */
case class Score(name: String, clazz: Int, score: Int)
object Demo08_WindowFunction {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    val scores: Array[Score] = Array(Score("a1", 1, 80),
      Score("a2", 1, 78),
      Score("a3", 1, 95),
      Score("a4", 2, 74),
      Score("a5", 2, 92),
      Score("a6", 3, 99),
      Score("a7", 3, 99),
      Score("a8", 3, 45),
      Score("a9", 3, 55),
      Score("a10", 3, 78),
      Score("a11", 3, 100))
    import spark.implicits._
    val df: DataFrame = sc.makeRDD(scores).toDF("name", "class", "score")

    // sql over() 函数
    df.createOrReplaceTempView("score_t")
    spark.sql("select *, count(*) over() from score_t").show()
    spark.sql("select *, count(*) over(partition by class) from score_t").show()
    spark.sql("select *, rank() over(partition by class order by score desc) from score_t").show()

    sc.stop()
  }
}

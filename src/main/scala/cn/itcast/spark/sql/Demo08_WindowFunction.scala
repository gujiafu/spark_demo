package cn.itcast.spark.sql

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
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    //2.加载数据
    import spark.implicits._
    val scoreDF: DataFrame = sc.makeRDD(Array(
      Score("a1", 1, 80),
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
    ).toDF("name", "class", "score")
    // 将数据注册成表
    scoreDF.createOrReplaceTempView("t_scores")

    /**
     * 聚合窗口函数演示
     */
    spark.sql("SELECT name, class, score, COUNT(*) OVER() FROM t_scores").show()
    spark.sql("SELECT name, class, score, COUNT(*) OVER(PARTITION BY class) FROM t_scores").show()

    /**
     * 排序窗口函数演示
     * row_number
     * rank
     * dense_rank
     */
    spark.sql("SELECT name, class, score, ROW_NUMBER() OVER(ORDER BY score) FROM t_scores").show()
    spark.sql("SELECT name, class, score, RANK() OVER(ORDER BY score) FROM t_scores").show()
    spark.sql("SELECT name, class, score, DENSE_RANK() OVER(ORDER BY score) FROM t_scores").show()
    spark.sql("SELECT name, class, score, ROW_NUMBER() OVER(PARTITION BY class ORDER BY score) FROM t_scores").show()


    sc.stop()
  }
}

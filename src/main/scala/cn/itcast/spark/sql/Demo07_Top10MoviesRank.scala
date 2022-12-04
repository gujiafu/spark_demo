package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:08
 * Desc: 电影评分TOP10 演示
 */
object Demo07_Top10MoviesRank {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 读取数据
    val rdd: RDD[(Int, Int, Double)] = sc.textFile("data/input/rating_100k.data")
      .map(line => {
        val arr: Array[String] = line.split("\t")
        (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
      })

    // 转变成DF
    import spark.implicits._
    val df: DataFrame = rdd.toDF("userID", "movieID", "rating")

    /**
     * SQL Style
     */
    df.createOrReplaceTempView("movies")
    val result: DataFrame = spark.sql(
      """
        |SELECT
        |movieID,
        |ROUND(AVG(rating), 2) AS avg_rating
        |FROM movies
        |GROUP BY movieID
        |HAVING COUNT(userID) > 200
        |ORDER BY avg_rating DESC
        |LIMIT 10
        |""".stripMargin)
    result.show()

    /**
     * DSL Style
     */
    // 在DSL中, 一些函数, 定义在spark的一个叫做Functions包中, 所以隐式转换要导入即可
    import org.apache.spark.sql.functions._
    val resultDSL: Dataset[Row] = df.groupBy($"movieID") // 先分组
      .agg( // agg 聚合 可以写多个聚合在里面
        count("userID").as("cnt"),
        round(avg($"rating"), 2).as("avg_rating")
      ).filter($"cnt" > 200)
      .select($"movieID", $"avg_rating")
      .orderBy($"avg_rating".desc)
      .limit(10)

    resultDSL.show()

    sc.stop()
  }
}

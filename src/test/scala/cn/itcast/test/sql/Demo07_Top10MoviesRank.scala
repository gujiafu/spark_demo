package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:08
 * Desc: 电影评分TOP10 演示
 */
object Demo07_Top10MoviesRank {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    val rdd: RDD[(Int, Int, Double)] = sc.textFile("data/input/rating_100k.data").map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    })
    import spark.implicits._
    val df: DataFrame = rdd.toDF("userId", "movieId", "rating")

    // sql 方式
    df.createOrReplaceTempView("movie_t")
    spark.sql(
      """
        |select
        |movieId,
        |round(avg(rating), 2) as avg_rating
        |from
        |movie_t
        |group by movieId
        |having count(userId) > 200
        |order by avg_rating desc
        |limit 10
        |""".stripMargin).show()

    // dsl 方式
    import org.apache.spark.sql.functions._
    df.groupBy($"movieId").agg(
      count("userId").as("cnt"),
      round(avg("rating"), 2).as("avg_rating")
    ).where($"cnt" > 200)
      .select($"movieId", $"avg_rating")
      .orderBy($"avg_rating".desc)
      .limit(10)
      .show()


    sc.stop()
  }
}

package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 20:42
 * Desc: 用SparkSQL完成RDD中的WordCount案例
 */
object Demo06_WordCountWithSparkSQL {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    import spark.implicits._
    val rdd: RDD[String] = sc.textFile("data/input/words.txt").flatMap(_.split(" "))
    val df: DataFrame = rdd.toDF("word")
    df.printSchema()
    df.show()

    // sql 风格
    df.createOrReplaceTempView("words_t")
    spark.sql("select word, count(*) as cnt from words_t group by word").show()

    // dsl 风格
    df.groupBy($"word")
      .count()
      .withColumnRenamed("count", "cnt")
      .orderBy($"cnt".desc)
      .show()

    sc.stop()
  }
}

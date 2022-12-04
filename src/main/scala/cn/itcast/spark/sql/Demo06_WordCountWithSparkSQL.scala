package cn.itcast.spark.sql

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
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    val wordsRDD: RDD[String] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
    import spark.implicits._
    val df: DataFrame = wordsRDD.toDF("word")

    /**
     * SQL风格
     */
    df.createOrReplaceTempView("words")
    spark.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()


    /**
     * DSL风格
     */
    df.groupBy($"word")
      .count()      // count后的列名 默认叫做count
      .withColumnRenamed("count", "cnt")  // 对DataFrame或者DataSet表的列执行改名
      .orderBy($"cnt".desc)
      .show()

    sc.stop()
  }
}

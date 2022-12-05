package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 21:35
 * Desc: SparkSQL定义用户自定义函数
 */
object Demo09_CustomUserFunction {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    val df: DataFrame = spark.read.text("data/input/udf.txt").toDF("word")
    df.createOrReplaceTempView("word_t")

    // 方式1
    import org.apache.spark.sql.functions._
    val udf1: UserDefinedFunction = udf((word: String) => word.toUpperCase)

    // 方式2
    val udf2: UserDefinedFunction = spark.udf.register(
      "to_upperCase",
      (word: String) => word.toUpperCase)

    // sql 风格
    spark.sql("select word, to_upperCase(word) from word_t").show()

    // dsl 风格
    import spark.implicits._
    df.select(udf1($"word")).show()
    df.select(udf2($"word")).show()


    sc.stop()
  }
}

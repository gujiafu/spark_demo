package cn.itcast.spark.sql

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
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    import spark.implicits._
    val df: DataFrame = sc.textFile("data/input/udf.txt").toDF("word")
    df.createOrReplaceTempView("words")
    // 定义方式1:(需要导入隐式转换)
    import org.apache.spark.sql.functions._
    val lowCaseToUpperCase1: UserDefinedFunction = udf(
      // 这里面要求是一个函数, 由于是UDF, 一条数据输入, 一条数据返回
      (word: String) => word.toUpperCase
    )

    // 定义方式2:
    val udf2: UserDefinedFunction = spark.udf.register(
      "lowCaseToUpperCase2", // UDF的名称
      (word: String) => word.toUpperCase // UDF函数内容
    )

    /**
     * 在SQL风格中使用UDF
     */
    // 在SQL风格中, 只能使用spark.udf.register方式定义的UDF函数
//    spark.sql("SELECT lowCaseToUpperCase1(word) FROM words").show()
    spark.sql("SELECT lowCaseToUpperCase2(word) FROM words").show()

    /**
     * DSL Style
     * DSL 两个风格都可以用.
     */
    df.select(lowCaseToUpperCase1($"word")).show()
    df.select(udf2($"word")).show()

    sc.stop()
  }
}

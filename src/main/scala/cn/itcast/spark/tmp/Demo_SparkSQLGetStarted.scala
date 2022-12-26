package cn.itcast.spark.tmp

import cn.itcast.spark.sql.Person
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-06 0006 22:02
 * Desc: 先体验一下在Spark中写SQL的感觉
 */
object Demo_SparkSQLGetStarted {
  def main(args: Array[String]): Unit = {
    // 0. Init env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 构建DF
    val personRDD: RDD[Person] = sc.textFile("data/input/person.txt")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        Person(arr(0).toInt, arr(1), arr(2).toInt)
      })
    // RDD转换为DataFrame
    // 需要导入SparkSession对象中的implicits内的全部内容, 这是隐式转换.
    import spark.implicits._
    val df: DataFrame = personRDD.toDF

    // 执行SQL步骤1: 将DF注册为表格
    df.createOrReplaceTempView("t_person")
    val resultDF: DataFrame = spark.sql("SELECT * FROM t_person WHERE age < 30")

    resultDF.printSchema();
    resultDF.show()

    resultDF.write.text("output/my-work-result.txt")

    sc.stop()
  }
}

/**
 在大数据环境中, 我们代码看起来简单
 但是数据量是不同的.
 */
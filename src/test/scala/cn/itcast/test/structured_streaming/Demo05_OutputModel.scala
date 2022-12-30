package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 21:55
 * Desc: 演示3种输出模式
 */
object Demo05_OutputModel {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // source
    val csvDF: DataFrame = spark.readStream
      .format("csv")
      .schema("name string, age int, hobby string")
      .option("sep", ";")
      .load("data/input/persons")

    // transform
    csvDF.createOrReplaceTempView("hobby_t")
//    val resultDF: DataFrame = spark.sql(
//      """
//        |select name, hobby, count(*) as cnt, max(age) max_age
//        |from hobby_t
//        |group by name, hobby
//        |having name like '%tom%'
//        |order by cnt desc, max_age
//        |limit 6
//        |""".stripMargin)

//    val resultDF: DataFrame = spark.sql(
//      """
//        |select name, hobby, age
//        |from hobby_t
//        |where name like '%tom%' or name like '%jack%'
//        |limit 10
//        |""".stripMargin)

    val resultDF: DataFrame = spark.sql(
      """
        |select name, hobby, count(*) as cnt, max(age) max_age
        |from hobby_t
        |group by name, hobby
        |having name like '%tom%'
        |""".stripMargin)


    // sink
    resultDF.writeStream
      .format("console")
//      .outputMode(OutputMode.Complete())
//      .outputMode(OutputMode.Append())
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

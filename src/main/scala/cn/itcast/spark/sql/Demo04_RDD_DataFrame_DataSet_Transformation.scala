package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 19:52
 * Desc: 演示RDD\DataFrame\DataSet之间相互转换
 */
object Demo04_RDD_DataFrame_DataSet_Transformation {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    // 先得到一个RDD[Person]
    val personRDD: RDD[Person] = sc.textFile("data/input/person.txt")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        Person(arr(0).toInt, arr(1), arr(2).toInt)
      })

    /**
     * RDD转DF和DS
     */
    // RDD -> DF
    // 先导入隐式转换
    import spark.implicits._
    val df1: DataFrame = personRDD.toDF()
    // RDD -> DS
    val ds1: Dataset[Person] = personRDD.toDS()

    /**
     * DF 转 RDD  和 DS
     */
    // DF -> RDD(要注意, RDD的泛型是[Row],如果泛型不是你想要的 需要自行替换泛型)
    val rdd1: RDD[Row] = df1.rdd
    // DF -> DS
    val ds2: Dataset[Person] = df1.as[Person]

    /**
     * DS 转 RDD 和 DF
     */
    // DS -> RDD(DS转RDD 泛型会保留)
    val rdd2: RDD[Person] = ds1.rdd
    // DS -> DF
    val df3: DataFrame = ds1.toDF()

    sc.stop()
  }
}

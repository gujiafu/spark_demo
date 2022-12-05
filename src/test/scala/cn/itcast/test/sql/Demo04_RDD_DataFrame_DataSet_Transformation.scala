package cn.itcast.test.sql

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
    // 初始环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    val rdd: RDD[Person] = sc.textFile("data/input/person.txt").map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    // RDD -> dataFrame
    import spark.implicits._
    val df1: DataFrame = rdd.toDF()
    // RDD -> dataSet
    val ds1: Dataset[Person] = rdd.toDS()

    // dataFrame -> rdd
    val rdd1: RDD[Row] = df1.rdd
    // dataFrame -> dataSet
    val ds2: Dataset[Person] = df1.as[Person]

    // dataSet -> rdd
    val rdd3: RDD[Person] = ds1.rdd
    // dataSet -> dataFrame
    val df3: DataFrame = ds1.toDF()

    sc.stop()
  }
}

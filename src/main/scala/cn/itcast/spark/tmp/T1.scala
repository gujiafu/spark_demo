package cn.itcast.spark.tmp

import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-06 0006 19:19
 * Desc: 
 */
object T1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()


    val read: DataFrameReader = spark.read
  }
}

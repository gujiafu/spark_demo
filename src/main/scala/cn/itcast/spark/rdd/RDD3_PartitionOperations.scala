package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 19:24
 * Desc: 演示Spark 应用在整个分区之上的算子
 */
object RDD3_PartitionOperations {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志级别为WARN,减少一些不必要的日志的输出

    // 1. 从文件中读取数据
    val data: RDD[String] = sc.textFile("data/input/words.txt")

    val wordsRDD: RDD[String] = data.flatMap(_.split(" "))

    // mapPartition算子
    // map方法: 将RDD中的每个元素,都传递给 提供的函数去计算, 得到每一次的返回值收集到新RDD中返回
    // mapPartition算子: 将RDD中的每个分区中的数据 传递给提供的函数去计算.
//    wordsRDD.map((ele: String) => ele -> 1)
    val wordWithOneRDD: RDD[(String, Int)] = wordsRDD.mapPartitions((iter: Iterator[String]) => {
      iter.map(_ -> 1)
    })


    // reducePartition算子, 一次搞一整个分区
    wordWithOneRDD.reduceByKey(_ + _)
      .foreachPartition((iter: Iterator[(String, Int)]) => {
        iter.foreach(println)
      })

    sc.stop()
  }
}
/**
 生产中 推荐 mapPartition 和 foreachPartition 因为性能好
 */

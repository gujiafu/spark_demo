package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 19:24
 * Desc: 演示Spark 应用在整个分区之上的算子
 */
object RDD3_PartitionOperations {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd2: RDD[String] = rdd.flatMap(x => x.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.mapPartitions(_.map(_ -> 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)
    rdd4.foreachPartition(_.foreach(println(_)))

    rdd.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .foreach(println(_))

    sc.stop()

  }
}
/**
 生产中 推荐 mapPartition 和 foreachPartition 因为性能好
 */

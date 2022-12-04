package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 19:14
 * Desc: 演示Spark中的对RDD进重新分区算子
 */
object RDD2_RePartitionOperations {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志级别为WARN,减少一些不必要的日志的输出

    // 1. 创建一个RDD
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5), 5)
    println(s"RDD1分区数: ${rdd1.getNumPartitions}")

    // 2. 对RDD的分区数量进行修改
    // 方法1:rePartition方法, rePartition方法是Transformation类型,因为返回新RDD
    // rePartition方法可以增加也可以减少分区数.
    // 一定要注意, 不是对原有rdd进行修改, 而是返回新分区数的新rdd
    val rdd2: RDD[Int] = rdd1.repartition(3)  // 减少分区
    val rdd3: RDD[Int] = rdd1.repartition(10)                      // 增加分区
    println(s"RDD2分区数: ${rdd2.getNumPartitions}")
    println(s"RDD3分区数: ${rdd3.getNumPartitions}")

    // 方法2:coalesce方法: 默认仅减少分区用
    val rdd4: RDD[Int] = rdd1.coalesce(1)
    // 如果要增加分区, shuffle参数需要给true
    val rdd5: RDD[Int] = rdd1.coalesce(6, true)
    println(s"RDD4分区数: ${rdd4.getNumPartitions}")
    println(s"RDD5分区数: ${rdd5.getNumPartitions}")

    sc.stop()
  }
}

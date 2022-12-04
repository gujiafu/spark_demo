package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 19:14
 * Desc: 演示Spark中的对RDD进重新分区算子
 */
object RDD2_RePartitionOperations {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8))

    val rdd2: RDD[Int] = rdd.repartition(2)
    val rdd3: RDD[Int] = rdd.repartition(20)

    println(s"分区数 ${rdd.getNumPartitions}")
    println(s"重分区数 ${rdd2.getNumPartitions}")
    println(s"重分区数 ${rdd3.getNumPartitions}")

    sc.stop()
  }
}

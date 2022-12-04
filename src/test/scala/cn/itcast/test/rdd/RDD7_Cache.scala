package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 14:42
 * Desc: 演示RDD的持久化操作
 * 1. 缓存
 * 2. CheckPoint
 */
object RDD7_Cache {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(_ -> 1)

    rdd3.persist(StorageLevel.MEMORY_AND_DISK)

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)

    rdd3.foreach(x => println(x))
    rdd4.foreach(x => println(x))

    rdd3.unpersist()

    sc.setCheckpointDir("data/ck")
    rdd4.checkpoint()

    sc.stop()
  }
}

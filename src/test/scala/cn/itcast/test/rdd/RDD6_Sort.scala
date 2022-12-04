package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 20:27
 * Desc: 演示SparkRDD的排序算子
 */
object RDD6_Sort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[(Int, String)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .map(_.swap)

    val rdd1: RDD[(Int, String)] = rdd.sortByKey()
    rdd1.foreach(x => println(s" sortByKey ${x}"))

    val rdd2: RDD[(Int, String)] = rdd.sortByKey(false)
    rdd2.foreach(x => println(s" sortByKey-false ${x}"))

    val rdd3: RDD[(Int, String)] = rdd.sortByKey(true, 1)
    rdd3.foreach(x => println(s" sortByKey-1 ${x}"))

    val rdd4: RDD[(Int, String)] = rdd.sortBy(_._1, true, 1)
    rdd4.foreach(x => println(s" sortBy ${x}"))

    val rdd5: Array[(Int, String)] = rdd.top(2)(Ordering.by(_._1))
    rdd5.foreach(x => println(s" top ${x}"))



  }
}

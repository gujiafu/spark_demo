package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 19:38
 * Desc: 
 */
object RDD4_AggregateOperations {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 非 key 聚合
    val rddNoKey: RDD[Int] = sc.parallelize(1 to 10)
    val rdd1: Double = rddNoKey.sum()
    val rdd2: Int = rddNoKey.reduce(_ + _)
    val rdd3: Int = rddNoKey.fold(0)(_ + _)
    val rdd4: Int = rddNoKey.aggregate(0)(_ + _, _ + _)

    println(rdd1)
    println(rdd2)
    println(rdd3)
    println(rdd4)

    // 带 key 聚合
    val rdd: RDD[(String, Int)] = sc.textFile("data/input/words.txt").flatMap(_.split(" ")).map(_ -> 1)

    val rdd5: RDD[(String, Int)] = rdd.groupByKey().map(x => x._1 -> x._2.sum)
    val rdd6: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    val rdd7: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    val rdd8: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)

    rdd5.foreach(x => println(s"groupByKey ${x}"))
    rdd6.foreach(x => println(s"reduceByKey ${x}"))
    rdd7.foreach(x => println(s"foldByKey ${x}"))
    rdd8.foreach(x => println(s"aggregateByKey ${x}"))

    sc.stop()

  }
}

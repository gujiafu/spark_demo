package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 20:27
 * Desc: 演示SparkRDD的排序算子
 */
object RDD6_Sort {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志级别为WARN,减少一些不必要的日志的输出

    // textFile 默认2个分区
    val fileRDD: RDD[String] = sc.textFile("data/input/words.txt")
    val wordCountResultRDD: RDD[(Int, String)] = fileRDD.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .map(_.swap)  // 元组的swap方法 可以将元组进行反转

    // 对单词计数结果进行排序
    // sortByKey
    // 默认是对分区内进行排序
    val r1: RDD[(Int, String)] = wordCountResultRDD.sortByKey()  // 默认按照key 升序
    val r2: RDD[(Int, String)] = wordCountResultRDD.sortByKey(false) // 降序
    // 如果要完成整体排序,需要加一个参数,控制分区数
    val r3: RDD[(Int, String)] = wordCountResultRDD.sortByKey(false, 1)
//    wordCountResultRDD.coalesce(1).sortByKey(false)
    r1.foreach(x => println(s"r1升序的内容:$x"))
    r2.foreach(x => println(s"r2降序的内容:$x"))
    r3.foreach(x => println(s"r3 1个分区后降序的内容:$x"))


    // sortBy 跟scala一样
    // 参数1 是按照谁排序  参数2 是升序或者降序  参数3 排序的分区数
    val r4: RDD[(Int, String)] = wordCountResultRDD.sortBy(_._1, ascending = false, numPartitions = 1)
    r4.foreach(x => println(s"r4 降序1个分区后的结果: $x"))



    // top 方法, 降序取TOP2
    // top 是action, 会将RDD的全部数据 塞入Driver的内存中 执行排序 取TOP N
    // top 方法用于 预期结果数据比较小的时候用, 大数据就别用了.
    val t1: Array[(Int, String)] = wordCountResultRDD.top(2)(Ordering.by(_._1))
    t1.foreach(x => println(s"t1 降序取TOP2后的结果: $x"))
  }
}

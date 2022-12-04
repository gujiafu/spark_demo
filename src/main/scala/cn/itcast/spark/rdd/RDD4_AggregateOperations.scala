package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 19:38
 * Desc: 
 */
object RDD4_AggregateOperations {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志级别为WARN,减少一些不必要的日志的输出

    // 1. 准备数据
    val rddNoKey: RDD[Int] = sc.parallelize(1 to 10)

    /*
    没有key的聚合
     */
    // 求和
    val sumResult: Double = rddNoKey.sum()
    // reduce 求和
    val reduceSumResult: Int = rddNoKey.reduce(_ + _)
    // fold 求和
    val foldSumResult: Int = rddNoKey.fold(0)(_ + _)
    // aggregate 求和, 参数列表2 传入2个函数, 函数1:分区内如何聚合,  函数2: 分区之间如何聚合
    val aggregateSumResult: Int = rddNoKey.aggregate(0)(_ + _, _ + _)
//    rddNoKey.aggregate(0)((x,y) => x + y, (x, y) => x + y)

    println(s"Result1: $sumResult")
    println(s"Result2: $reduceSumResult")
    println(s"Result3: $foldSumResult")
    println(s"Result4: $aggregateSumResult")


    /*
    有key的数据的聚合
     */
    val rddWithKey: RDD[(String, Int)] = sc.textFile("data/input/words.txt")
      .flatMap(_.split(" "))
      .map(_ -> 1)

    // 分组算子 聚合求单词计数, groupByKey 表示自动按照key分组
    val groupedRDD: RDD[(String, Iterable[Int])] = rddWithKey.groupByKey()
    val result1: RDD[(String, Int)] = groupedRDD.map(tuple => tuple._1 -> tuple._2.sum)

    // reduceByKey 算子, 自动按照key 来聚合. 无需进行分组, 直接给聚合逻辑即可
    val result2: RDD[(String, Int)] = rddWithKey.reduceByKey(_ + _)

    // aggregateByKey 算子, 自动按照key 来聚合. 无需进行分组, 直接给聚合逻辑即可
    val result3: RDD[(String, Int)] = rddWithKey.aggregateByKey(0)(_ + _, _ + _)

    // foldByKey 算子, 自动按照key 来聚合. 无需进行分组, 直接给聚合逻辑即可
    val result4: RDD[(String, Int)] = rddWithKey.foldByKey(0)(_ + _)

    result1.foreach(x => println(s"Result1结果: $x"))
    result2.foreach(x => println(s"Result2结果: $x"))
    result3.foreach(x => println(s"Result3结果: $x"))
    result4.foreach(x => println(s"Result4结果: $x"))
  }
}

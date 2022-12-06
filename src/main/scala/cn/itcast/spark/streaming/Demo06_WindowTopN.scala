package cn.itcast.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 20:21
 * Desc: 通过滑动窗口(滑动小于窗口长度,会重复计算),来计算TopN热门单词
 */
object Demo06_WindowTopN {
  def main(args: Array[String]): Unit = {
    // 0. 构建执行环境入口
    // RDD: SparkContext
    // SQL: SparkSession
    // Streaming: StreamingContext
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    // 参数1:SparkContext, 参数2:微批处理时间间隔
    // 流计算入口对象: StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Duration(5000L))

    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val windowedDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKeyAndWindow((_:Int) + (_:Int), Duration(20000L), Duration(10000L))

    // 按照窗口聚合好数据后, 继续执行排序, 做TopN
    // transform方法是对DStream中的每一个RDD执行计算
    // 比如 map, map是对每一条数据进行计算, mapPartition:对每一个分区执行计算, transform:对每一个RDD执行计算(mapRDD)
    // 我们说DStream是一批RDD在时间线上的集合, 也就是DStream中可以有多个RDD,那么, 窗口后的DStream中有几个RDD?
    // DStream加了窗口之后, 只有一个RDD了
    windowedDStream.transform(rdd => {
      // 因为只有一个RDD, 所以对当前RDD内部执行排序, 等同于DStream的全局排序
      println("----------")
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, ascending = false, 1)
      sortedRDD.take(3).foreach(x => println("TOP3的word: " + x))
      sortedRDD
    }).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}


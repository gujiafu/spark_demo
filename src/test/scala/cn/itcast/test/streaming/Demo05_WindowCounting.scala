package cn.itcast.test.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 16:27
 * Desc: 演示SparkStreaming的窗口计算
 */
object Demo05_WindowCounting {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(3000L))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val resultDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Duration(6000L), Duration(3000L))
    resultDS.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}

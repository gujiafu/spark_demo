package cn.itcast.test.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 16:27
 * Desc: 演示流计算入门WordCount
 * liunx 执行 nc -lk 9999
 * window 执行  nc -l -p 9999
 */
object Demo01_WordCount {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(5000L))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val ds: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)
    ds.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)

  }
}

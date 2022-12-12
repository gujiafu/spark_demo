package cn.itcast.test.streaming

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
    val session: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(2000L))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val windowDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Duration(20000L), Duration(4000L))

    windowDS.transform(rdd =>{
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false, 1)
      sortRDD.take(3).foreach(r => println(s"TOP 10 排名 ${r}"))
      sortRDD
    }).print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}


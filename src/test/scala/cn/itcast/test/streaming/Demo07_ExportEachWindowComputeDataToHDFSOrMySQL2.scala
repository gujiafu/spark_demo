package cn.itcast.test.streaming

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{MapWithStateDStream, ReceiverInputDStream}

import java.sql.{Connection, Date, DriverManager, Timestamp}
import java.text.SimpleDateFormat

object Demo07_ExportEachWindowComputeDataToHDFSOrMySQL2 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(5000L))
    ssc.checkpoint("data/ck")

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val mappingFunction: (String, Option[Int], State[Int]) => (String, Int) = (key, currentValue, state) =>{
      val newValue: Int = currentValue.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(newValue)
      key -> newValue
    }

    val windowedDStream: MapWithStateDStream[String, Int, Int, (String, Int)] = socketDS.flatMap(_.split(" "))
      .map(_ -> 1)
      .window(Duration(5000L))
      .reduceByKey(_ + _)
      .mapWithState(StateSpec.function(mappingFunction))

    windowedDStream.print()

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    windowedDStream.foreachRDD((rdd: RDD[(String, Int)], time :Time) =>{
      val windowTime = format.format(new Date(time.milliseconds))
      println(s"窗口时间 ${windowTime}")

      if (!rdd.isEmpty()) {
        rdd.foreachPartition((iter: Iterator[(String, Int)]) =>{
          val conn: Connection = DriverManager.getConnection("jdbc:mysql://192.168.88.163:3306/mytest?characterEncoding=UTF-8", "root", "123456")

          val sql = "REPLACE INTO t_hotwords(time, word, count, time_stamp) VALUES (?, ?, ?, ?);"
          val ps = conn.prepareStatement(sql)
          iter.foreach(i =>{
            ps.setTimestamp(1, new Timestamp(time.milliseconds))
            ps.setString(2, i._1)
            ps.setInt(3, i._2)
            ps.setLong(4, time.milliseconds)
            ps.addBatch()
          })
          ps.executeBatch()
          ps.close()
          conn.close()
        })
      }

    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}

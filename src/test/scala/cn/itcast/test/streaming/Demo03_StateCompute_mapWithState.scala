package cn.itcast.test.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext, Time}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 17:17
 * Desc: 有状态计算之:mapWithState API
 */
object Demo03_StateCompute_mapWithState {
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(5000L))
    ssc.checkpoint("data/ck/stream")


    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val mappingFunction: (String, Option[Int], State[Int]) => (String, Int) = (key, value, state) =>{
      val newValue: Int = state.getOption().getOrElse(0) + value.getOrElse(0)
      state.update(newValue)
      (key, newValue)
    }

    val result: MapWithStateDStream[String, Int, Int, (String, Int)] = socketDS.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .mapWithState(StateSpec.function(mappingFunction))
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)

  }
}

package cn.itcast.test.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 17:17
 * Desc: 有状态计算之:UpdateStateByKey API
 */
object Demo02_StateCompute_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(5000L))
    ssc.checkpoint("data/streaming/ck")

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val updateFunc: (Seq[Int], Option[Int]) => Option[Int] = (currentValues, historyValue) =>{
      if(currentValues.nonEmpty){
        Option(currentValues.sum + historyValue.getOrElse(0))
      }else{
        historyValue
      }
    }

    val resultDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" "))
      .map(_ -> 1).updateStateByKey(updateFunc)
    resultDS.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}

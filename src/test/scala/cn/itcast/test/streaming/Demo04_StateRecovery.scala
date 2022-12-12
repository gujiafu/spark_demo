package cn.itcast.test.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 19:03
 * Desc: 演示SparkStreaming的状态恢复
 */
object Demo04_StateRecovery {

  val checkPointPath = "data/ck/recovery"

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPointPath, creatingFunc)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }

  val creatingFunc: () => StreamingContext = () => {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(5000))
    ssc.checkpoint(checkPointPath)

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val updateFunc: (Seq[Int], Option[Int]) => Option[Int] = (currentValues, historyValue) => {
      if (currentValues.nonEmpty) {
        Option(currentValues.sum + historyValue.getOrElse(0))
      } else {
        historyValue
      }
    }

    val resultDS: DStream[(String, Int)] = socketDS.flatMap(_.split(" "))
      .map(_ -> 1)
      .updateStateByKey(updateFunc)

    resultDS.print()
    ssc
  }
}

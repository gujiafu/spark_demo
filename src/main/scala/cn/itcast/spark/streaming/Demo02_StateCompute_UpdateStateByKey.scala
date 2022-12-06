package cn.itcast.spark.streaming

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
    val ssc: StreamingContext = new StreamingContext(sc, Duration(10000L))

    // 要做有状态计算, 必须开启CheckPoint
    ssc.checkpoint("data/ck") // 必须是StreamingContext的CheckPoint方法

    // 获取数据
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


    /**
     * 先定义更新历史状态API要求的函数
     */
    val updateFunc: (Seq[Int], Option[Int]) => Option[Int] = (currentValues, historyValue) => {
      if (currentValues.nonEmpty) { // 因为是流处理, 不是每个批次都有数据
        // 进来if表示有数据, 和历史状态进行聚合, 返回新的结果出去
        Option(currentValues.sum + historyValue.getOrElse(0))
      }else{
        // 表示没数据
        historyValue
      }
    }

    val resultDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
      .map(_ -> 1)
      //      .reduceByKey() reduceByKey 只对当前批次执行聚合, 不理会历史状态, 所以要换API
      .updateStateByKey(updateFunc)   // 就通过这个函数 完成历史状态的更新
      // 通过updateStateByKey(会和历史结果进行关联)替换reduceByKey(只关心当前批次)

    resultDStream.print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}

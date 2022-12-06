package cn.itcast.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 17:17
 * Desc: 有状态计算之:mapWithState API
 */
object Demo03_StateCompute_mapWithState {
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
     * 准备mapWithState中需要的函数
     */
      // 参数2 的当前value,是一个 不是seq集合了, 要求我们给的数据当前值是已经预聚合好的当前批次结果
      val mappingFunction:(String, Option[Int], State[Int]) => (String, Int) = (key, currentValue, state) => {
        // 通过当前value和历史value得到新value
        val newValue: Int = state.getOption().getOrElse(0) + currentValue.getOrElse(0)
        // 将新value更新回 历史State对象中
        state.update(newValue)
        // 返回结果
        key -> newValue   // 要注意, 这个返回值要求带key
      }

    val resultDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)   // 对于mapWithState 需要做预聚合,当前批次内的聚合通过reduceByKey来做
      .mapWithState(StateSpec.function(mappingFunction))

    resultDStream.print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}

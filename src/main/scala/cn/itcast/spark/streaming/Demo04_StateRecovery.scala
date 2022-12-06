package cn.itcast.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 19:03
 * Desc: 演示SparkStreaming的状态恢复
 */
object Demo04_StateRecovery {
  val CKP_PATH = "data/ck"
  def main(args: Array[String]): Unit = {
    /**
     * 状态的恢复, 在Streaming恢复的是 StreamingContext对象
     * getOrCreate方法可以做到:
     * 1. 如果有CheckPoint保存,从CheckPoint中恢复StreamingContext的内容
     * 2. 如果没有CheckPoint,那么就构建一个新的StreamingContext对象
     */
    // 参数1: CheckPoint的路径 用来从CheckPoint中恢复
    // 参数2: 一个函数, 这个函数返回一个StreamingContext
    val ssc: StreamingContext = StreamingContext.getOrCreate(CKP_PATH, createStreamingContext)


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }

  // SSC 如果要初始化完成, 必须有ssc的后续执行逻辑, 等同于后面没有DAG
  val createStreamingContext:() => StreamingContext = () => {
    // 在这个函数中创建StreamingContext对象即可
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Duration(5000L))
    ssc.checkpoint(CKP_PATH)

    // 获取数据
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


    /**
     * 状态恢复, 只能用updateStateByKey
     * mapWithState不支持.
     */

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

//    /**
//     * 准备mapWithState中需要的函数
//     */
//    // 参数2 的当前value,是一个 不是seq集合了, 要求我们给的数据当前值是已经预聚合好的当前批次结果
//    val mappingFunction:(String, Option[Int], State[Int]) => (String, Int) = (key, currentValue, state) => {
//      // 通过当前value和历史value得到新value
//      val newValue: Int = state.getOption().getOrElse(0) + currentValue.getOrElse(0)
//      // 将新value更新回 历史State对象中
//      state.update(newValue)
//      // 返回结果
//      key -> newValue   // 要注意, 这个返回值要求带key
//    }
//
//    val resultDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
//      .map(_ -> 1)
//      .reduceByKey(_ + _)   // 对于mapWithState 需要做预聚合,当前批次内的聚合通过reduceByKey来做
//      .mapWithState(StateSpec.function(mappingFunction))
//
//    resultDStream.print()

    ssc
  }
}

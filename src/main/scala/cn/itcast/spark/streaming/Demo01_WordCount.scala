package cn.itcast.spark.streaming

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


    // 1. 准备数据(注意,流计算要使用流数据(有界数据也可以))
    // 典型的流数据源, 有kafka
    // 或者使用socket连接一个socketserver,从socketserver中获取源源不断的数据
    // 获取一个文本流的socket通道
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 2. 进行单词计数, 假设每一行数据都是空格切分的
    val resultDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)

    // 3. 调用Output算子(Action算子)启动任务
    resultDStream.print()   // 将数据输出到控制台

    // 4. 启动流计算任务(和批处理不同, 流计算任务需要代码中有 启动控制)
    ssc.start()   // 启动流计算程序

    // 流计算有一个特性:一旦开始, 不知道何时结束(因为数据是无界的), 就是程序一旦启动, 不知道什么时候会停
    ssc.awaitTermination()    // 阻塞主线程(阻塞Driver, 等executor端执行流任务) 等待程序停止(正常情况下 不会停)
    // 参数1: true表示executor端执行完成, 同步停止SparkContext(Driver)
    // 参数2: true表示,是否优雅停止程序(在异常情况下, 是否处理完手头的事情再停止自己)
    ssc.stop(true, true)
  }
}

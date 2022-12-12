package cn.itcast.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import java.sql.{Connection, Date, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 20:38
 * Desc: 演示将每个窗口的计算结果写出到HDFS或者MySQL中
 */
object Demo07_ExportEachWindowComputeDataToHDFSOrMySQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    // 参数1:SparkContext, 参数2:微批处理时间间隔
    // 流计算入口对象: StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Duration(5000L))
    ssc.checkpoint("data/ck")
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

    val windowedDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" "))
      .map(_ -> 1)
      .window(Duration(5000L))    // 只加窗口的方法, 参数1是窗口长度, 参数2是滑动距离, 如果只给参数1, 窗口长度 == 滑动距离
      .reduceByKey(_ + _) // 窗口内的预聚合不要忘记了.
      .mapWithState(StateSpec.function(mappingFunction))

    // 写出HDFS和MySQL之前 先打印出来看看
    windowedDStream.print()


    /**
     * 写出数据, 使用foreachRDD方法
     * 总结:
     * map 处理每一行数据 并返回
     * foreach: 处理每一行数据 无返回
     * mapPartition: 处理每一个分为 带返回
     * foreachPartition: 处理每一个分区 无返回
     * transform: 处理每一个DStream中的RDD, 有返回
     * foreachRDD: 处理每一个DStream中的RDD, 无返回
     */

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    windowedDStream.foreachRDD((rdd:RDD[(String, Int)], time: Time) => {
      // rdd 就是被处理的每一个RDD, time是当前这个窗口的时间属性
      val windowTime: String = sdf.format(new Date(time.milliseconds))
      println(s"本窗口的时间为: $windowTime")

      // 开始执行数据输出
      // 先判断RDD是否为空, 因为流数据, 可能某个窗口内没数据, 没数据就别处理了
      if (!rdd.isEmpty()) {
        // rdd有数据才处理

        // 写出HDFS
        rdd.saveAsTextFile(s"hdfs://node1:8020/tmp/rddoutput-${time.milliseconds}")

        // 写出MySQL
        rdd.foreachPartition((iter: Iterator[(String, Int)]) => {
          val conn: Connection = DriverManager.getConnection("jdbc:mysql://node3:3306/mytest?characterEncoding=UTF-8","root","123456")
          val sql:String = "REPLACE INTO `t_hotwords` (`time`, `word`, `count`) VALUES (?, ?, ?);"
          val ps: PreparedStatement = conn.prepareStatement(sql)//获取预编译语句对象
          iter.foreach(t=>{
            val word: String = t._1
            val count: Int = t._2
            ps.setTimestamp(1,new Timestamp(time.milliseconds) )
            ps.setString(2,word)
            ps.setInt(3,count)
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

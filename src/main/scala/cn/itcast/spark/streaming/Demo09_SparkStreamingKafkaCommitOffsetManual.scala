package cn.itcast.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

import java.lang

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 21:24
 * Desc: 演示SparkStreaming和kafka集成(手动提交offset模式)
 */
object Demo09_SparkStreamingKafkaCommitOffsetManual {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    // 参数1:SparkContext, 参数2:微批处理时间间隔
    // 流计算入口对象: StreamingContext
    val ssc: StreamingContext = new StreamingContext(sc, Duration(1000L))
    ssc.checkpoint("data/ck")
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val kafkaParams: Map[String, Object] = Map[String, Object](
      // broker地址列表
      "bootstrap.servers" -> "node1:9092",
      "group.id" -> "streaming-kafka",
      // key 序列化器 和 value序列化器(网络中传输数据 必须要序列化)
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化规则,按照String进行序列化
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化规则
      "auto.offset.reset" -> "latest", //offset重置位置, 指的是:如果没有找到offset用, 这个设置指的是从最新的地方读取
//      "auto.commit.interval.ms" -> "1000", //自动提交的时间间隔
      "enable.auto.commit" -> (false: lang.Boolean) //是否自动提交偏移量
    )

    // 处理kafka, 创建一个直连kafka的stream流
    // ConsumerRecord是kafka中消费出来的数据封装类, 这里面记录了 一条数据的: key value(数据本身) 分区号, offset topic等信息
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,  // 本地策略, PreferConsistent表示 executor的分区会尽量和kafka的分区一一对应
      ConsumerStrategies.Subscribe[String, String](Array("streaming-kafka2"), kafkaParams)
    )

    // 我们通过foreachRDD 将每个kafka中的每条数据手动提交
    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {   // 确保消费到数据再处理
        rdd.foreach(x => println(s"消费的数据,topic:${x.topic()}, 分区:${x.partition()}, offset:${x.offset()}, key:${x.key()}, 数据:${x.value()}"))


        // 手动提交的逻辑
        // 将DStream转成CanCommitOffsets, 这个对象有方法可以用于手动提交offset
        val offsets: CanCommitOffsets = kafkaDStream.asInstanceOf[CanCommitOffsets]

        // 将rdd转换成HasOffsetRanges对象, 然后调用offsetRanges方法 取得一个数组
        // 这个数组中记录的就是 这个rdd中的数据 对应的offset 对应的topic 对应的partition 都在这个array里面记录的
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 通过async 提交即可 手动提交
        offsets.commitAsync(ranges)
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true, true)
  }
}

package cn.itcast.edu.mock


import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

/**
 * 发送数据到kafka的生产者线程对象
 */
class KafkaProducerThread extends Thread {
  val logger = LoggerFactory.getLogger(classOf[KafkaProducerThread])

  val props = new Properties()
  props.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
  props.setProperty("ack", "1")
  props.setProperty("batch.size", "16384")
  props.setProperty("linger.ms", "5")
  props.setProperty("buffer.memory", "33554432")
  props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
  val gson = new Gson()

  override def run(): Unit = {
    while (true) {
      val question = Simulator.genQuestion()
      val jsonString = gson.toJson(question)

      producer.send(new ProducerRecord[String, String]("edu", jsonString), new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            logger.info("当前偏移量：" + metadata.partition() + "-" + metadata.offset() + "\n数据发送成功：" + jsonString)
          } else {
            logger.error("数据发送失败：" + exception.getMessage)
          }
        }
      })
      Thread.sleep(300)
    }
  }
}
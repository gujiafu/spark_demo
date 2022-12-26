package cn.itcast.spark.structured_streaming.kafka.iot

import cn.itcast.spark.structured_streaming.kafka.iot.MockIotDatas.DeviceData
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-17 0017 16:36
 * Desc: SQL风格从kafka消费数据进行处理IOT数据
 *
 * {"device":"device_73","deviceType":"bigdata","signal":77.0,"time":1618648888346}
 */
object IOTStreamingAnalysisSQL {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._


    // 从kafka读取数据
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "iotTopic")
      .load()


    /**
     * 数据处理
     *  1）、信号强度大于30的设备；
     *  2）、各种设备类型的数量；
     *  3）、各种设备类型的平均信号强度；
     */
    val dataDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    val deviceDS: Dataset[DeviceData] = dataDS
      // 先过滤异常数据
      .filter(StringUtils.isNotBlank(_))
      .map(
        // 如果能有一个API 能快速的将JSON字符串, 转变成样例类, 也挺美.
        line => {
          JSON.parseObject(line, classOf[DeviceData])
        }
      )

    // 需求1:信号强度大于30的设备
    deviceDS.createOrReplaceTempView("device")
    val result1: DataFrame = spark.sql(
      """
        |SELECT * FROM device WHERE signal > 30
        |""".stripMargin)
    result1.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    // 需求2: 各种设备类型的数量和平均信号强度
    spark.sql(
      """
        |SELECT
        |deviceType,
        |COUNT(*) AS cnt,
        |ROUND(AVG(signal), 2) AS avg_signal
        |FROM device
        |GROUP BY deviceType
        |""".stripMargin
    ).writeStream
      .format("console")
      .outputMode("complete")
      .start()

    // 阻塞
    spark.streams.awaitAnyTermination()

    //    kafkaDF.selectExpr("CAST(value AS STRING)")
    //      .writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .option("truncate", false)
    //      .start()
    //      .awaitTermination()

//    sc.stop()
  }
}

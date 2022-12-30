package cn.itcast.test.structured_streaming.kafka.iot

import cn.itcast.spark.structured_streaming.kafka.iot.MockIotDatas.DeviceData
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
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

    /**
     * 数据处理
     *  1）、信号强度大于30的设备；
     *  2）、各种设备类型的数量；
     *  3）、各种设备类型的平均信号强度；
     */

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 输入
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "iotTopic")
      .option("startingOffsets", "earliest")
      .load()

    // 转换
    val deviceDF: Dataset[DeviceData] = kafkaDF.selectExpr("CAST(value as string)")
      .as[String]
      .filter(StringUtils.isNoneBlank(_))
      .map(line => {
        JSON.parseObject(line, classOf[DeviceData])
      })

    deviceDF.printSchema()

    deviceDF.createOrReplaceTempView("device_t")
    val resultDF: DataFrame = spark.sql(
      """
        |select
        |deviceType,
        |count(*) as cnt,
        |round(avg(signal), 2) as avg_signal
        |from device_t
        |where signal >30
        |group by deviceType
        |order by cnt
        |""".stripMargin)

    // 输出
    resultDF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

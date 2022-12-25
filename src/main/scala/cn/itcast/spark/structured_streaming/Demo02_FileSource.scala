package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 20:57
 * Desc: 演示StructuredStreaming的File数据源
 */
object Demo02_FileSource {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    // 经常用到的两个隐式转换我们也写上
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // File Source
    // 流场景中由于数据是源源不断产生的.
    // 文件是 有界数据, 那么流场景和文件的交集在就与, 文件是源源不断产生的(这样满足流数据的定义).
    val csvDF: DataFrame = spark.readStream
      .format("csv")
      .option("sep", ";")
      .schema("name String, age Int, hobby String")
      .load("data/input/persons")   // 读取一个文件夹

    csvDF.printSchema()

    // input processing output
    // 数据的输出
    csvDF.writeStream
      .outputMode("append")
      .format("console")    // 输出到控制台
      .start()      // 启动
      .awaitTermination() // 阻塞

    sc.stop()

    // readStream.format 控制输入源
    // writeStream.format 控制输出源
    // 以前的df.show 不能用, 因为不是流的标准输出源, 而是批的输出源.
    // 尽管api 在 但是不能用.

  }
}

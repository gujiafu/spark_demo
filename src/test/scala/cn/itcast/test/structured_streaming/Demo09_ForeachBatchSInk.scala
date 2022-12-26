package cn.itcast.test.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-15 0015 21:05
 * Desc: 演示StructuredStreaming的ForeachBatch Sink
 */
object Demo09_ForeachBatchSInk {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // source
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 999)
      .load()

    // transform
    val wordDF: DataFrame = df.flatMap(row => row.getString(0).split(" ")).toDF("word")
    val resultDF: Dataset[Row] = wordDF.groupBy($"word").count().orderBy($"count".desc)

    // sink
    resultDF.writeStream
      .outputMode(OutputMode.Complete())
      .foreachBatch((df:DataFrame, batchId:Long) =>{
        println("-----------------")
        println(s"============batchID ${batchId}")
        println("-----------------")
        df.show()

        df.write
          .format("jdbc")
          .option("url", "jdbc:mysql://192.168.88.166:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "bigdata.word_t")
          .mode(SaveMode.Overwrite)
          .save()

      })
      .start()
      .awaitTermination()

    sc.stop()
  }
}

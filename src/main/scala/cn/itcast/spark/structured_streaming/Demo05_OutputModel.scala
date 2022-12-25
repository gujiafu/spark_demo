package cn.itcast.spark.structured_streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-13 0013 21:55
 * Desc: 演示3种输出模式
 */
object Demo05_OutputModel {
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


    // 数据输入 Socket
    val socketDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 处理数据, 以WordCount为例DataFrame == DataSet[Row]
    val wordDF: DataFrame = socketDF.flatMap(row => row.getString(0).split(" ")).toDF("word")

    val dslResult: Dataset[Row] = wordDF.groupBy($"word")
      .count()
//      .orderBy($"count".desc)

    dslResult.writeStream
      .format("console")
      /**
       * Append: 只输出新行, 不支持聚合
       *  append 本质上就是对 没有key的数据, 做无状态计算用. 可以满足来一条输出一条,简单的无状态处理
       * Complete: 只支持聚合, 输出全部无界DF中缓存的数据
       *  复杂有状态计算, 对全量结果进行输出
       * Update: 只输出有变化的(新行和更改) ,有聚合的话也可以, 没有聚合也可以(当Append用), 但是不支持排序算子
       *  复杂有状态计算, 最更新的结果进行输出.
       */
//      .outputMode(OutputMode.Append())
//      .outputMode(OutputMode.Complete())
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()

    sc.stop()
  }
}

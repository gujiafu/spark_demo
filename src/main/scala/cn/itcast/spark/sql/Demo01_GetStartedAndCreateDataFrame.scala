package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-06 0006 20:53
 * Desc: 演示SparkSQL入门初体验以及如何创建DataFrame(通过本地的数据文件来构建)
 */
object Demo01_GetStartedAndCreateDataFrame {
  def main(args: Array[String]): Unit = {
    // 0. 初始化执行环境的入口
    // 在Spark2.0后,其实执行入口更换为:SparkSession对象
    // 构建SparkSession 这个新的执行入口对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      //      .config() // 设置一些应用程序相关的配置
      .getOrCreate() // 通过这个方法从Builder对象中构建SparkSession执行入口
    // 演示: 通过SparkSession这个新入口获取以前用的 SparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    /**
     * 演示获取DataFrame的各种方式
     */
    // 获取一个单列的DataFrame, text方法只会返回一个列, 列名叫做value
    val df1: DataFrame = spark.read.text("data/input/person.txt")
    df1.printSchema();
    df1.show()

    // 获取多列的DataFrame, 也就是以CSV的形式去读取数据
    val df2: DataFrame = spark.read
      .option("sep", " ") // sep选项指定分隔符
      .option("header", false) // header 指定是否有CSV标头
      .option("encoding", "UTF-8") // 指定字符编码集
      .schema("id INT, name STRING, age INT")
      .csv("data/input/person.txt")
    df2.printSchema();
    df2.show()

    // 获取JSON数据
    val df3: DataFrame = spark.read
      .option("encoding", "UTF-8")
      .option("lineSep", "\n") // 指定行的分隔符
      .option("dateFormat", "yyyy-MM-dd") // 指定日期的识别格式
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") // 时间戳的识别格式
      .json("data/input/person.json")
    df3.printSchema();
    df3.show()

    // 读取Parquet数据(Parquet是一种序列化的数据文件,是列式存储的)
    val df4: DataFrame = spark.read
      .parquet("data/input/users.parquet")
    df4.printSchema();
    df4.show()

    /**
     * 统一访问的API
     * spark.read.text("path")
     * spark.read.format("text").load("path")
     *
     * spark.read.csv("path")
     * spark.read.format("csv").load("path")
     *
     * spark.read.json("path")
     * spark.read.format("json").load("path")
     *
     * 这些两两配对的API是等价的.
     */

    /**
     * 如何将DataFrame写出
     */
    // 写出单列的文本
    df1.write
      .mode(SaveMode.Overwrite) // 写出模式, Overwrite表示是覆盖
      .text("data/output/textoutput") // text方法表示写出单列的文本

    // 写出数据到csv
    df2.write
      .mode(SaveMode.Overwrite) // 写出模式, Overwrite表示是覆盖
      .option("sep", "|") // 指定写出的分隔符
      .csv("data/output/csv")

    // 写出到Json数据
    df3.write
      .mode(SaveMode.Overwrite) // 写出模式, Overwrite表示是覆盖
      .json("data/output/json")

    // 写出到Parquet
    df3.write
      .mode(SaveMode.Overwrite) // 写出模式, Overwrite表示是覆盖
      .parquet("data/output/parquet")

    // 写出的方法本质上就是如下这些方法;
//    df3.write.format("csv").save("")
//    df3.write.format("json").save("")
//    df3.write.format("text").save("")
//    df3.write.format("parquet").save("")



    sc.stop()
  }
}

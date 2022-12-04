package cn.itcast.spark.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 14:42
 * Desc: 将Spark代码提交到集群运行
 */
object WordCountYARN {
  def main(args: Array[String]): Unit = {
    // 0. 获取Spark的环境入口
    val conf: SparkConf = new SparkConf().setAppName("WordCountLocal")
      // 指定Spark的Master是谁
      // - Local 表示本地运行, local[2] 表示可以用本地2个CPU核心来开启JVM进程
      // 或者比如 local[*] 表示使用本机全部的资源
      // - ip:7077 表示连接StandAlone集群
      // - yarn 表示提交到yarn运行
      .setMaster("yarn")
    // SparkContext就是程序的入口
    val sc: SparkContext = new SparkContext(conf)
    // 1. 读取数据, YARN模式需要读取hdfs的文件 本地的是读取不到
    val file: RDD[String] = sc.textFile("hdfs://node1:8020/wordcount/input/words.txt")
    // 2. 执行计算
    val result: RDD[(String, Int)] = file.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)
    // 3. 输出结果
    println(result.collect.mkString(","))




    // ---------------------------------------------

//    List(1,2,3,4,5,6,7)   // 如果程序启动, 这个List的数据 就在一个JVM内存中
//
//    RDD(1,2,3,4,5,6,7).map()  // 如果程序启动,假设有3个Executor,可能
//    /**
//     * executor1 内存中记录 1,2,3
//     * Executor2 内存中记录 4,5
//     * Executor 3 内存中记录 6,7
//     */
//
//


  }
}

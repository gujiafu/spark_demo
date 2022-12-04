package cn.itcast.spark.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 14:42
 * Desc: 
 */
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    // 0. 获取Spark的环境入口
    val conf: SparkConf = new SparkConf().setAppName("WordCountLocal")
      // 指定Spark的Master是谁
      // - Local 表示本地运行, local[2] 表示可以用本地2个CPU核心来开启JVM进程
      // 或者比如 local[*] 表示使用本机全部的资源
      // - ip:7077 表示连接StandAlone集群
      // - yarn 表示提交到yarn运行
      .setMaster("local[*]")
    // SparkContext就是程序的入口
    val sc: SparkContext = new SparkContext(conf)
    // 1. 读取数据
    val file: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd1: RDD[String] = file.flatMap(_.split(" "))  // 比如rdd1 不被回收, 那就可以从rdd1从新计算了.
    val rdd2: RDD[(String, Int)] = rdd1.map(_ -> 1)
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    // 3. 输出结果
    println(rdd3.collect.mkString(","))

    // rdd2 想要重复用. rdd2前面的流程 都要重跑一遍.
    // 核心原因就是 rdd2 被用过了就丢了. 想要在用,需要重新生产出rdd2
    // 游没有办法不要重新生产rdd2呢?
    // 有: 在创建rdd2后, 打上标记,告知spark 不要删即可.
    println(rdd2.collect().mkString(","))
    Thread.sleep(10000000L)
  }
}

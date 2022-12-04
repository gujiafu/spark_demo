package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 17:26
 * Desc: 演示如何创建RDD
 */
object RDD1_Create {
  def main(args: Array[String]): Unit = {
    // 0. 构建Spark的执行环境入口
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]") // 运行在本地模式 IDEA中直接跑
      .setAppName(this.getClass.getSimpleName)  // 给app设置个名称
      // Spark执行环境入口, SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 将日志级别设置到WARN级别,避免过多的日志.

    // RDD的创建

    // 创建方式1.1, SparkContext对象的parallelize方法 将本地Scala集合 转换为分布式RDD
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))  // 默认分区数(动态的),是CPU核数
    val rdd2: RDD[Int] = sc.parallelize(List(1, 2, 3), 6)  // 指定分区数构建RDD
    println(s"RDD1的分区数:${rdd1.getNumPartitions}")
    println(s"RDD2的分区数:${rdd2.getNumPartitions}")

//    val l = List(1, 2, 3)
//    l.map()   // map称之为 方法  这个map是list对象的一个方法
//    rdd1.map()  // rdd中map称之为算子, 我们称map是rdd的一个算子(本质还是方法)


    // 创建方式1.2 SparkContext对象的makeRDD方法 将本地Scala集合 转换为分布式RDD
    // makeRDD和parallelize方法一致
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3))
    val rdd4: RDD[Int] = sc.makeRDD(List(1, 2, 3), 6)

    // 创建方式2: 从文件中读取数据, 转变成分布式RDD,
    // textFile 如果不设定分区数, 默认是2个分区读取数据
    val rdd5: RDD[String] = sc.textFile("data/input/words.txt")
    // textFile支持设定分区数,但是设定的过大无效.
    val rdd6: RDD[String] = sc.textFile("data/input/words.txt", 100)  // 也支持设定分区数
    // textFile 支持从HDFS读取
    val hdfsRDD: RDD[String] = sc.textFile("hdfs://node1:8020/wordcount/input/words.txt")
    println(s"RDD5的分区数:${rdd5.getNumPartitions}")
    println(s"RDD6的分区数:${rdd6.getNumPartitions}")

    // 创建方式3: 读取一个`文件夹`中的全部数据, 将全部数据封装到分布式RDD中
    // textFile方法支持从文件夹读取. 默认分区数是文件的数量
    val rdd7: RDD[String] = sc.textFile("data/input/ratings10")
    // textFile minPartitions参数表示最小分区数, 这个参数确保分区不低于(不是等于)你给定的值.
    val rdd8: RDD[String] = sc.textFile("data/input/ratings10", 100)
    // textFile 你设定的最小分区数如果小于文件数量,那么 最小是文件数量
    val rdd9: RDD[String] = sc.textFile("data/input/ratings10", 3)
    println(s"RDD7的分区数:${rdd7.getNumPartitions}")
    println(s"RDD8的分区数:${rdd8.getNumPartitions}")
    println(s"RDD9的分区数:${rdd9.getNumPartitions}")

    // 创建方式4: 也是从文件夹中读取, 这个是针对存储小文件的文件夹的专用方法.
    // wholeTextFiles 默认是2分区
    val rdd10: RDD[(String, String)] = sc.wholeTextFiles("data/input/words.txt")
    // wholeTextFiles 最大分区数是文件数量
    val rdd11: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10", 100)
    // wholeTextFiles 3个分区 小于文件数量, 可以正常设置
    val rdd12: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10", 3)
    // wholeTextFiles 也支持从HDFS读取
    val hdfsRDD2: RDD[(String, String)] = sc.wholeTextFiles("hdfs://node1:8020/wordcount/input/words.txt", 3)
    println(s"RDD10的分区数:${rdd10.getNumPartitions}")
    println(s"RDD11的分区数:${rdd11.getNumPartitions}")
    println(s"RDD12的分区数:${rdd12.getNumPartitions}")
    // 停止运行Spark程序,关闭资源
    sc.stop()

  }
}

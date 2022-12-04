package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 14:42
 * Desc: 演示RDD的持久化操作
 * 1. 缓存
 * 2. CheckPoint
 */
object RDD7_Cache {
  def main(args: Array[String]): Unit = {
    // 0. 获取Spark的环境入口
    val conf: SparkConf = new SparkConf().setAppName("WordCountLocal")
      .setMaster("local[*]")
    // SparkContext就是程序的入口
    val sc: SparkContext = new SparkContext(conf)
    // 1. 读取数据
    val file: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd1: RDD[String] = file.flatMap(_.split(" ")) // 比如rdd1 不被回收, 那就可以从rdd1从新计算了.

    /**
     * 以缓存的形式去持久化RDD
     */
    val rdd2: RDD[(String, Int)] = rdd1.map(_ -> 1)
    // RDD2如果想要重复用, 可以对其持久化, 告知Spark 不要删我
//    rdd2.cache() // 将RDD2 缓存到内存中, 不会被删除掉. cache 调用的是 persist(StorageLevel.MEMORY_ONLY) 表示仅在内存中存储
//    rdd2.persist() // 不加参数调用,还是persist(StorageLevel.MEMORY_ONLY)
    rdd2.persist(StorageLevel.MEMORY_AND_DISK)  // 最常用的
    /**
     * val NONE = new StorageLevel(false, false, false, false) 不保存
     * val DISK_ONLY = new StorageLevel(true, false, false, false) 仅磁盘
     * val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2) 仅磁盘 2个副本
     * val MEMORY_ONLY = new StorageLevel(false, true, false, true) 仅内存
     * val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2) 仅内存 2个副本
     * val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false) 仅内存 以序列化存储
     * val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)仅内存 以序列化存储 2个副本
     * val MEMORY_AND_DISK = new StorageLevel(true, true, false, true) 内存 + 硬盘
     * val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)内存 + 硬盘 2副本
     * val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false) 内存 + 硬盘 以序列化保存
     * val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2) 内存 + 硬盘 以序列化保存 2副本
     * val OFF_HEAP = new StorageLevel(true, true, true, false, 1)  堆外内存, 放入其它分布式内存框架中
     */
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    // 如果rdd2 后续不想要了. 可以释放缓存, 节省内存和磁盘空间
    rdd2.unpersist()  // 释放缓存

    /**
     * 以CheckPoint的方式去缓存RDD
     * CheckPoint的方式 没有释放的操作. 保存了就是保存了.
     */
    sc.setCheckpointDir("hdfs://node1:8020/wordcount/output/ck")    // 指定检查点路径.可以是本地路径, 也可以是HDFS路径
    rdd2.checkpoint() // 会将数据保存到 指定的 CheckPoint的检查点路径去

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

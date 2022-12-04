package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-30 0030 19:50
 * Desc:  演示一下Spark的外部数据源,SequenceFile ObjFile
 */
object RDD9_ExternalDataSource {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 准备测试数据
    val data: RDD[(Int, Int)] = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_ -> 1)

    // 先演示如何写出seq 和 obj文件
    // 保存成SequenceFile
    data
      .coalesce(1)  // 设置为1个分区 就有1个文件输出
      .saveAsSequenceFile("data/output/seqoutput")

    // 保存成Obj文件
    data.coalesce(1).saveAsObjectFile("data/output/objoutput")


    // 从序列化文件中读取 数据构建RDD
    // 我们读取的数据,必须以KV的形式存储, 要告知文件中存储的K是什么V是什么类型
    val seqRDD: RDD[(Int, Int)] = sc.sequenceFile[Int, Int]("data/output/seqoutput")

    // 从标准序列化中读取(Obj文件)
    val objRDD: RDD[(Int, Int)] = sc.objectFile[(Int, Int)]("data/output/objoutput")

    seqRDD.foreach(x => println("SEQ:" + x))
    objRDD.foreach(x => println("OBJ:" + x))

    sc.stop()
  }
}

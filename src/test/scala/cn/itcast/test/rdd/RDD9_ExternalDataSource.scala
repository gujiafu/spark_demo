package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-30 0030 19:50
 * Desc:  演示一下Spark的外部数据源,SequenceFile ObjFile
 */
object RDD9_ExternalDataSource {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd: RDD[(Int, Int)] = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_ -> 1)

    // SequenceFile 保存
//    rdd.repartition(1).saveAsSequenceFile("data/output/seqoutput")
    // ObjectFile 保存
//    rdd.repartition(2).saveAsObjectFile("data/output/objoutput")

    // SequenceFile 读取
    val rdd1: RDD[(Int, Int)] = sc.sequenceFile("data/output/seqoutput")
    rdd1.foreach(x => println(x))

    // ObjectFile 读取
    val rdd2: RDD[(Int, Int)] = sc.objectFile("data/output/objoutput")
    rdd2.foreach(x => println(x))

  }
}

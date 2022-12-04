package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD1_Create {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // parallelize
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val rdd2: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6), 6)
    println(s"parallelize 分区 ${rdd1.getNumPartitions}")
    println(s"parallelize 分区 ${rdd2.getNumPartitions}")

    // makeRDD
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))
    val rdd4: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 6)
    println(s"makeRDD 分区 ${rdd3.getNumPartitions}")
    println(s"makeRDD 分区 ${rdd4.getNumPartitions}")

    // textFile 文件
    val rdd5: RDD[String] = sc.textFile("data/input/words.txt")
    val rdd6: RDD[String] = sc.textFile("data/input/words.txt", 1)
    val rdd7: RDD[String] = sc.textFile("data/input/words.txt", 100)
    println(s"textFile文件 分区 ${rdd5.getNumPartitions}")
    println(s"textFile文件 分区 ${rdd6.getNumPartitions}")
    println(s"textFile文件 分区 ${rdd7.getNumPartitions}")

    // textFile 文件夹
    val rdd8: RDD[String] = sc.textFile("data/input/ratings10")
    val rdd9: RDD[String] = sc.textFile("data/input/ratings10", 1)
    val rdd10: RDD[String] = sc.textFile("data/input/ratings10", 100)
    println("textFile文件夹 分区 " + rdd8.getNumPartitions)
    println(s"textFile文件夹 分区 ${rdd9.getNumPartitions}")
    println(s"textFile文件夹 分区 ${rdd10.getNumPartitions}")

    //
//    val rdd11: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10/part-01")
//    println(s"wholeTextFiles文件 ${rdd11.getNumPartitions}")

  }
}

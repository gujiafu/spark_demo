package cn.itcast.test.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wordCountLocal").setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")
//    val rdd: RDD[(String, Int)] = context.textFile("data/input/words.txt")
    val rdd: RDD[(String, Int)] = context.textFile("hdfs://node1:8020/wordcount/input/words.txt")
      .flatMap(_.split(" "))
      .map(_ -> 1)
      .reduceByKey(_ + _)

    println("分区数=" + rdd.partitions.length)

    rdd.foreach(x => println(x))

    Thread.sleep(100000)


  }
}

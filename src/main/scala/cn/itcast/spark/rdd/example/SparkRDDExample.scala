package cn.itcast.spark.rdd.example

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util
import scala.collection.mutable

/**
 * Author: itcast caoyu
 * Date: 2021-04-01 0001 19:53
 * Desc: SparkRDD阶段的搜索引擎日志分析小demo
 */

case class SoGouDefine(time:String, userID:String, sousuoKey:String, resultRank:Int, clickRank:Int, url:String)

object SparkRDDExample {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    /**
     * 需求有3:
     * 1. 搜索关键词的统计
     * 2. 用户搜索次数的统计
     * 3. 搜索时间段的统计
     */
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // 1. 读取日志数据
    val fileRDD: RDD[String] = sc.textFile("data/input/SogouQ.sample")
    // 将文本转换成了样例类对象
    val dataRDD: RDD[SoGouDefine] = fileRDD
      // 先简单的过滤数据
      .filter(line => line != null && line.trim.split("\\s+").length == 6)
      .mapPartitions(iter => {
        iter.map(line => {
          val arr: Array[String] = line.trim.split("\\s+")
          // 将数据封装到样例类中
          SoGouDefine(
            arr(0),
            arr(1),
            arr(2).replaceAll("\\[|\\]", ""),
            arr(3).toInt,
            arr(4).toInt,
            arr(5)
          )
        })
      })

    dataRDD.foreach(println)
    // 这个RDD后续会被使用多次, 所以 缓存它
    dataRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // 需求1
    println("==关键词统计需求1==")
    val wordRDD: RDD[String] = dataRDD.mapPartitions(iter => {
      iter.flatMap((record: SoGouDefine) => {
        val terms: util.List[Term] = HanLP.segment(record.sousuoKey)
        terms.asScala.map(_.word)
          .filter(x => {
            // 过滤数据中的 + 和 .
            if (x == "+" || x == ".") false else true
          })
      })
    })

    val top10Word: Array[(String, Int)] = wordRDD.map(_ -> 1).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false, 1)
      .take(10) // take 取前N个元素  和top不同, top是自动排序取前N, take是直接取无排序

    top10Word.foreach(x => println(s"需求1:TOP10 搜索关键词是: $x"))



    // 需求2:用户搜索次数的统计
    println("==需求2:用户搜索次数统计==")
    val userKeyWordRDD: RDD[(String, Int)] = dataRDD.map(record => {
      (record.userID + "_" + record.sousuoKey) -> 1
    }).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false, 1)

    println(s"被搜索最多的TOP10: ${userKeyWordRDD.take(10).mkString(",")}")
    println(s"被搜索最多的:${userKeyWordRDD.take(1).mkString("")}")
    println(s"被搜索最少的:${userKeyWordRDD.sortBy(_._2, ascending = true, 1).take(1).mkString("")}")
    println(s"平均搜索次数:${userKeyWordRDD.map(_._2).mean()}")

    // 需求3: 时间段统计
    dataRDD.map(record => {
      // 取出时间中的分钟属性
      record.time.substring(3, 5) -> 1
    }).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false, 1)
      .foreach(x => println(s"热门分钟倒序输出:$x"))

    Thread.sleep(100000000L)
    sc.stop()
  }
}

package cn.itcast.test.rdd.example

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import scala.collection.JavaConverters._


object SparkRDDExample {

  def main(args: Array[String]): Unit = {
    // 初始环境
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 广播特殊符号
    val specCharList = List("+", ".")
    val specCharBroadcast: Broadcast[List[String]] = sc.broadcast(specCharList)

    /**
     * 1、准备数据 rdd
     */
    val fileRdd: RDD[String] = sc.textFile("data/input/SogouQ.sample")
    val logRDD: RDD[SogouQLog] = fileRdd.filter(line => {
      StringUtils.isNoneBlank(line) && line.trim.split("\\s+").length == 6
    }).mapPartitions(iter => {
      iter.map(line => {
        val arr: Array[String] = line.split("\\s+")
        val time: String = arr(0)
        val userId: String = arr(1)
        val keyWord: String = arr(2).trim.replace("[", "").replace("]", "")
        val resultRang: Integer = Integer.valueOf(arr(3))
        val clickRang: Integer = Integer.valueOf(arr(4))
        val url: String = arr(5)
        new SogouQLog(time, userId, keyWord, resultRang, clickRang, url)
      })
    })
    logRDD.foreach(println(_))
    // 持久化，后续使用不需要重新加工
    logRDD.persist(StorageLevel.MEMORY_AND_DISK)

    /**
     * 2、需求 关键字排名top10
     */
    val keyWordTop10: Array[(String, Int)] = logRDD.mapPartitions(iter => {
      iter.flatMap(log => {
        val terms: util.List[Term] = HanLP.segment(log.keyWord)
        terms.asScala.map(_.word.trim -> 1)
      })
    }).filter(ele => {
      !specCharBroadcast.value.contains(ele._1)
    })
      .reduceByKey(_ + _)
      .sortBy(_._2, false, 1)
      .take(10)
    keyWordTop10.foreach(x => println(s"关键字排名top10  ${x}"))


    /**
     * 3、需求 用户id_关键字 搜索排名top10
     */
    val userIDAndKeyWordTop10: Array[(String, Int)] = logRDD.mapPartitions(iter => {
      iter.map(log => {
        (log.userId + "_" + log.keyWord) -> 1
      })
    }).reduceByKey(_ + _)
      .sortBy(_._2, false, 1)
      .take(10)
    userIDAndKeyWordTop10.foreach(x => println(s"用户id_关键字 搜索排名top10 ${x}"))

    /**
     * 4、需求 根据时分区间 搜索点击次数 top10
     */
    val timeTop10: Array[(String, Int)] = logRDD.mapPartitions(iter => {
      iter.map(log => {
        (log.time.substring(0, 5)) -> log.clickRang
      })
    }).reduceByKey(_ + _)
      .sortBy(_._2, false, 1)
      .take(10)

    var index = 0
    for (item <- timeTop10){
      index = index +1
      println(s"根据时分区间 搜索点击次数前 top10 排名: ${index} ${item}")
    }
  }
}

case class SogouQLog(time: String, userId: String, keyWord: String, resultRang: Int, clickRang: Int, url: String)

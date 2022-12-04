package cn.itcast.test.rdd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 21:46
 * Desc: 演示SPark的广播变量 和 累加器
 */
object RDD8_ShareVariableAndAccumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val wordRDD: RDD[String] = sc.textFile("data/input/words2.txt")

    // 特殊符广播
    val specCharBroadcast: Broadcast[List[String]] = sc.broadcast(List(",", ".", "!", "#", "$", "%"))
    // 累加器
    val specCharAccumulator: LongAccumulator = sc.longAccumulator("specCharAccumulator")

    val result: RDD[(String, Int)] = wordRDD.filter(StringUtils.isNoneBlank(_))
      .flatMap(_.split("\\s+"))
      .filter(StringUtils.isNoneBlank(_))
      .filter(x => {
        if (specCharBroadcast.value.contains(x)) {
          specCharAccumulator.add(1)
          false
        } else {
          true
        }
      })
      .map(_ -> 1)
      .reduceByKey(_ + _)

    result.foreach(println(_))

    println(s"累加器  ${specCharAccumulator.value}")

    sc.stop()


  }
}

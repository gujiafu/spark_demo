package cn.itcast.spark.rdd

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
    // 0. 获取Spark的环境入口
    val conf: SparkConf = new SparkConf().setAppName("WordCountLocal")
      .setMaster("local[*]")
    // SparkContext就是程序的入口
    val sc: SparkContext = new SparkContext(conf)

    // 读取数据
    val datasRDD: RDD[String] = sc.textFile("data/input/words2.txt")

    // 字典数据, 记录的是特殊字符
    val list: List[String] = List(",", ".", "!", "#", "$", "%")
    /**
     * 需求: 提供一个字典, 字典中记录了一些规则 记录的规则就是一些特殊字符
     * 我们要做的是对words2.txt的数据,进行:
     * 1. 非特殊字符,做单词计数
     * 2. 求有多少个特殊字符
     */

    // 需要将字典数据,变成广播变量, 这样在关联的时候, 节省内存(一个executor只有一份)
    val broadCast: Broadcast[List[String]] = sc.broadcast(list)

    // 需求2需要做计数, 由于我们是分布式的集群, 如果做累加的话, 每个机器各自累加自己的
    // 我们需要有一个所有executor都共享的变量,用来做累加,这样确保不管多少
    // executor 我们的累加结果都是准确的.
    // 需要用到Spark提供的累加器功能, 就是完成分布式累加的.
    val acmlt: LongAccumulator = sc.longAccumulator("MyAccumulator")


    // 开始做业务计算
    val normalWord: RDD[String] = datasRDD
      // 先过滤掉一些异常数据,比如空行. 空格.
      .filter(StringUtils.isNotBlank(_))
      // 先将所有的单词 或者特殊字符 都取到
      .flatMap(_.trim.split("\\s+")) // 按照正则切分, 按照任意数量的空格切分
      // 通过filter来计算, 通过filter把特殊字符过滤掉, 在过滤掉的过程中完成累加
      .filter(
        value => {
          // 先将广播变量中的list取出来
          val list: List[String] = broadCast.value
          // 对比 元素是否是 特殊字符
          if (list.contains(value)) {
            // true 表明是特殊字符
            // 是特殊字符就进行累加
            acmlt.add(1) // 累加1 表示遇到了一个特殊字符串
            // 然后返回false 不要了
            false
          } else {
            // 表示是正常字符串
            true
          }
        }
      ) // 最终得到的都是正常单词, 同时特殊字符被累加了

    // 正常单词的wordcount结果
    val normalWordCountRDD: RDD[(String, Int)] = normalWord.map(_ -> 1).reduceByKey(_ + _)

    normalWordCountRDD.foreach(x => println(s"正常单词的WordCount结果是: $x"))

    // 特殊字符的累加结果
    println(s"特殊字符的数量是: ${acmlt.value}")

    sc.stop()


  }
}

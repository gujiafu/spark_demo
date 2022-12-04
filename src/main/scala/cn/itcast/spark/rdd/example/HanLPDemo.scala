package cn.itcast.spark.rdd.example

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import scala.collection.JavaConverters._
import java.util

/**
 * Author: itcast caoyu
 * Date: 2021-04-01 0001 19:47
 * Desc: 测试中文分词器的使用
 */
object HanLPDemo {
  def main(args: Array[String]): Unit = {
    val terms: util.List[Term] = HanLP.segment("博学谷的同学们都很帅")
    println(terms)
    // 导入import scala.collection.JavaConverters._ 使用隐式转换
    // 让Java的List 拥有asScala这个方法
    println(terms.asScala.map(_.word.trim))
  }
}

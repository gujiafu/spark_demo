package cn.itcast.test.rdd.example

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter

object HanLPDemo {
  def main(args: Array[String]): Unit = {
    val terms: util.List[Term] = HanLP.segment("博学谷的同学都不是那么帅")
    println(terms)

    terms.asScala.map(_.word.trim).foreach(println(_))
  }

}

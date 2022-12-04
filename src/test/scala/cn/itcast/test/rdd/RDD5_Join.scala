package cn.itcast.test.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 20:12
 * Desc: 演示Spark的Join算子, 将两个RDD进行关联的操作
 */
object RDD5_Join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val empRDD: RDD[(Int, String)] = sc.parallelize(Seq((1001, "z3"), (1002, "l4"), (1003, "w5"), (1004, "d6"), (1003, "b9")))
    val deptRDD: RDD[(Int, String)] = sc.parallelize(Seq((1001, "worker"), (1002, "teacher"), (1003, "docker")))

    empRDD.join(deptRDD).foreach(x => println(s" join ${x}"))
    empRDD.leftOuterJoin(deptRDD).foreach(x => println(s" leftOuterJoin ${x}"))
    empRDD.rightOuterJoin(deptRDD).foreach(x => println(s" rightOuterJoin ${x}"))
    empRDD.fullOuterJoin(deptRDD).foreach(x => println(s" fullOuterJoin ${x}"))



    empRDD.union(deptRDD).foreach(x => println(s" union ${x}"))

    sc.stop()

  }
}

package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 20:12
 * Desc: 演示Spark的Join算子, 将两个RDD进行关联的操作
 */
object RDD5_Join {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志级别为WARN,减少一些不必要的日志的输出

    // 模拟数据集
    val empRDD: RDD[(Int, String)] = sc.parallelize(
      // 数据是 (部门ID -> 员工名)
      Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhangliu"))
    )
    // 数据是:(部门ID -> 部门名称)
    val deptRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "sales"), (1002, "tech"), (1001, "sales2"))
    )

    // 两个RDD进行Join , Inner Join
    // 默认的管理条件就是按照Key来进行关联的
    val result1: RDD[(Int, (String, String))] = empRDD.join(deptRDD)

    result1.foreach(x => println("result1的数据是: " + x))

    // Left Join
    val result2: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)
    result2.foreach(x => println("左外关联的数据是: " + x))

    // right join
    val result3: RDD[(Int, (Option[String], String))] = empRDD.rightOuterJoin(deptRDD)
    result3.foreach(x => println("右外关联的数据是: " + x))

    // 全关联 - 笛卡尔集
    val result4: RDD[(Int, (Option[String], Option[String]))] = empRDD.fullOuterJoin(deptRDD)
    result4.foreach(x => println("全关联的数据是: " + x))

    // 合并, 两个RDD合成1个RDD
    val result5: RDD[(Int, String)] = empRDD.union(deptRDD)
    result5.foreach(x => println("合并的的数据是: " + x))

    sc.stop()
  }
}

package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Author: itcast caoyu
 * Date: 2021-04-10 0010 14:41
 * Desc: 演示IDEA中的Local模式的Spark 去连接Metastore获取表进行计算
 */
object Demo12_SparkOnHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      // 告知spark, hive数据仓库的默认保存HDFS路径是什么
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      // 告知spark, hive的metastore服务在哪里
      .config("hive.metastore.uris", "thrift://node3:9083")
      // 告知Spark, 我们现在是集成Hive模式. 请从hive的metastore中获取表信息
      .enableHiveSupport()  // 开关, 开启集成hive模式
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    spark.sql("SELECT * FROM itcast_ads.pmt_ads_info")
      .show(100, truncate = false)

    sc.stop()
  }
}

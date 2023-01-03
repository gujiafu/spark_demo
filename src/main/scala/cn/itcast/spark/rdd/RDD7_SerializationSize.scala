package cn.itcast.spark.rdd

import cn.itcast.spark.bean.HobbyBean
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-27 0027 14:42
 * Desc: 演示RDD的持久化序列化大小测试, 页面 打开 http://localhost:4040/storage/ 查看
 */
object RDD7_SerializationSize {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[HobbyBean]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val csvRDD: RDD[String] = sc.textFile("data/input/persons/file1.csv")
    csvRDD.setName("csvRDD")

    val hobbyRDD: RDD[HobbyBean] = csvRDD
      .filter(StringUtils.isNoneEmpty(_))
      .map(line => {
        val arr: Array[String] = line.split(";")
        HobbyBean(arr(0), Integer.valueOf(arr(1)), arr(2))
      })
    hobbyRDD.setName("hobbyRDD")

//        csvRDD.persist(StorageLevel.MEMORY_ONLY)
    csvRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    csvRDD.count()

//    hobbyRDD.persist(StorageLevel.MEMORY_ONLY)
    hobbyRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    hobbyRDD.count()

    Thread.sleep(999999999)
    sc.stop()
  }
}


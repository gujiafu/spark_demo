package cn.itcast.test.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Demo03_CreateDataSet {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // 直接 dataset
    val ds = spark.read.textFile("data/input/person.txt")
    ds.printSchema()
    ds.show()

    // rdd泛型 转 dataset
    val peopleRDD: RDD[People] = sc.textFile("data/input/person.txt").map(line => {
      val arr = line.split(" ")
      People(arr(0).toInt, arr(1), arr(2).toInt)
    })
    import spark.implicits._
    val ds1: Dataset[People] = peopleRDD.toDS()
    ds1.printSchema()
    ds1.show()

    // rdd非泛型 转 dataset
    val rdd2: RDD[(Int, String, Int)] = peopleRDD.map(p => (p.id, p.name, p.age))
    val ds2: Dataset[(Int, String, Int)] = rdd2.toDF("id", "name", "age").as[(Int, String, Int)]
    ds2.printSchema()
    ds2.show()

    sc.stop()
  }
}

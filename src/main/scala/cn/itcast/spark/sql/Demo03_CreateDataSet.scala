package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 19:37
 * Desc: 演示如何构建DataSet对象
 */

//case class Person(id:Int, name:String, age:Int)
object Demo03_CreateDataSet {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 从文本文件中读取数据获取DataSet
    val ds1: Dataset[String] = spark.read
      .textFile("data/input/person.txt")
    ds1.printSchema(); ds1.show();
    // 对于DataSet的获取 比如csv\json\parquet这些数据获取dataset
    // 是没有专用的方法的. 可以将数据先得到DataFrame 然后将DataFrame转换成DataSet即可


    // 从RDD中获取DataSet
    // 先得到一个RDD[Person]
    val personRDD: RDD[Person] = sc.textFile("data/input/person.txt")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        Person(arr(0).toInt, arr(1), arr(2).toInt)
      })
    // RDD 如何 得到DataSet
    import spark.implicits._
    val ds2: Dataset[Person] = personRDD.toDS()    // toDS就可以将RDD转换成DataSet
    ds2.printSchema(); ds2.show()

    // 没有样例类泛型的RDD转换成DataSet
    val tupleRDD: RDD[(Int, String, Int)] = personRDD.map(person => (person.id, person.name, person.age))
    val ds3: Dataset[(Int, String, Int)] =
      tupleRDD.toDF("id", "name", "age")
        .as[(Int, String, Int)]

    ds3.printSchema(); ds3.show()

    sc.stop()
  }
}

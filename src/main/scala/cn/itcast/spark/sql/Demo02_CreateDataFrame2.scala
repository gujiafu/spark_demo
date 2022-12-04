package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-06 0006 21:39
 * Desc: 演示构建DataFrame的方式(通过样例类来获取)
 */

case class Person(id:Int, name:String, age:Int)

object Demo02_CreateDataFrame2 {
  def main(args: Array[String]): Unit = {
    // 0. Init env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
     * 通过RDD(有样例类泛型的)来构建一个DataFrame
     */
    // 先得到一个RDD[Person]
    val personRDD: RDD[Person] = sc.textFile("data/input/person.txt")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        Person(arr(0).toInt, arr(1), arr(2).toInt)
      })
    // RDD转换为DataFrame
    // 需要导入SparkSession对象中的implicits内的全部内容, 这是隐式转换.
    import spark.implicits._
    val df1: DataFrame = personRDD.toDF

    // 通过带有样例类泛型的RDD转换来的DataFrame是可以识别Schema的
    df1.printSchema(); df1.show()


    /**
     * 通过没有样例类泛型的RDD转换一个DataFrame
     */
    // 把PersonRDD转变为:RDD[(Int, String, Int)]
    val tupleRDD: RDD[(Int, String, Int)] = personRDD.map(person => (person.id, person.name, person.age))
    // 元组泛型的RDD转变成DataFrame会没有列名,但是会识别列类型
    // 但是可以通过toDF方法手动指定列名的信息
    val df2: DataFrame = tupleRDD.toDF("id", "name", "age")
    df2.printSchema(); df2.show()


    /**
     * 手动指定Schema的方式来构建DataFrame
     * [了解] - 不怎么用 (最规范的方式)
     */
    // 读取数据, 得到一个RDD[Row]对象
    val rowRDD: RDD[Row] = personRDD.map(person => Row(person.id, person.name, person.age))
    val df3: DataFrame = spark.createDataFrame(rowRDD, StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, true),
        StructField("age", IntegerType, false)
      )
    ))

    df3.printSchema(); df3.show()

    sc.stop()
  }
}

package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Demo02_CreateDataFrame2 {
  def main(args: Array[String]): Unit = {
    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // RDD泛型 转换 dataframe
    val rdd: RDD[String] = sc.textFile("data/input/person.txt")
    val personRDD: RDD[Person] = rdd.map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    import spark.implicits._
    val df1: DataFrame = personRDD.toDF()
    df1.printSchema()
    df1.show()

    // RDD 非泛型 转换 dataframe
    val rdd2: RDD[(Int, String, Int)] = rdd.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0).toInt, arr(1), arr(2).toInt)
    })
    val df2: DataFrame = rdd2.toDF("id", "name", "age")
    df2.printSchema()
    df2.show(3)

    // RDD 标准 转换 dateFrame
    val rdd3: RDD[Row] = rdd.map(line => {
      val arr: Array[String] = line.split(" ")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })
    val df3: DataFrame = spark.createDataFrame(rdd3, StructType(
      Array(
        StructField("id", IntegerType, true)
        , StructField("name", StringType, true)
        , StructField("id", IntegerType, true)
      ))
    )
    df3.printSchema()
    df3.show()

    sc.stop()
  }
}

case class Person(id:Int, name:String, age:Int)

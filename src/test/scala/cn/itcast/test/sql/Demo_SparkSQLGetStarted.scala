package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Demo_SparkSQLGetStarted {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //读数据
    val rdd: RDD[People] = sc.textFile("data/input/person.txt").map(line => {
      val arr: Array[String] = line.split(" ")
      People(arr(0).toInt, arr(1), arr(2).toInt)
    })
    import spark.implicits._
    val df: DataFrame = rdd.toDF()
    df.createOrReplaceTempView("people_t")
    val resultDF: DataFrame = spark.sql("select * from people_t where age <30")
    resultDF.printSchema()
    resultDF.show()

    // 写数据
    resultDF.write.mode(SaveMode.Overwrite).json("data/output/people_t_json")

    // 关闭
    sc.stop()
  }
}

case class People(id: Int, name: String, age: Int)
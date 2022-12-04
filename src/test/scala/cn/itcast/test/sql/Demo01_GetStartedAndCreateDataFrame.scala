package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql._

object Demo01_GetStartedAndCreateDataFrame {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 读 txt
    val df1: DataFrame = spark.read.text("data/input/person.txt")
    df1.printSchema()
    df1.show()

    // 读 csv
    val df2: DataFrame = spark.read
      .option("sep", " ")
      .option("header", false)
      .option("encoding", "UTF-8")
      .schema("id int, name string, age int")
      .csv("data/input/person.txt")
    df2.printSchema()
    df2.show()

    // 读 json
//    val df3: DataFrame = spark.read.json("data/input/2015-03-01-11.json.gz")
//    df3.printSchema()
//    df3.show()
    val df4: DataFrame = spark.read.json("data/input/person.json")
    df4.printSchema()
    df4.show()

    // 读 parquet
    val df5: DataFrame = spark.read.parquet("data/input/users.parquet")
    df5.printSchema()
    df5.show()

    // 写 text
    df1.write.mode(SaveMode.Overwrite).text("data/output/textOutput")
    // 写 csv
    df2.write.option("sep", "|").mode(SaveMode.Overwrite).csv("data/output/csvOutput")
    // 写 json
    df4.write.mode(SaveMode.Overwrite).json("data/output/jsonOutput")
    // 写 parquet
    df5.write.mode(SaveMode.Overwrite).parquet("data/output/parquetOutput")


    sc.stop()
  }
}

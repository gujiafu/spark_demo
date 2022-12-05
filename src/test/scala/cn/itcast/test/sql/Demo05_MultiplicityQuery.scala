package cn.itcast.test.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 20:01
 * Desc: 演示SparkSQL的一些基本查询API 以及DSL和SQL两种不同风格的写法
 */
object Demo05_MultiplicityQuery {
  def main(args: Array[String]): Unit = {
    // 准备环境
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 准备数据
    import spark.implicits._
    val ds: Dataset[Person] = spark.read.textFile("data/input/person.txt").map(line => {
      val arr: Array[String] = line.split(" ")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })

    /**
     * 查询需求:
     * 1. 查看name字段
     * 2. 查看name和age字段
     * 3. 查询name和age字段,并将age结果+1
     * 4. 过滤年龄小于30岁
     * 5. 统计年龄小于30岁的人数
     * 6. 按年龄进行分组,并统计每个组的人数
     */

    /**
     * 以SQL风格来查询
     */
    ds.createOrReplaceTempView("person_t")
    // 1. 查看name字段
    spark.sql("select age from person_t").show()
    // 2. 查看name和age字段
    spark.sql("select age, name from person_t").show()
    // 3. 查询name和age字段,并将age结果+1
    spark.sql("select age, name, (age+1) as age2 from person_t").show()
    // 4. 过滤年龄小于30岁
    spark.sql("select * from person_t where age <30")
    // 5. 统计年龄小于30岁的人数
    spark.sql("select count(*) from person_t where age <30").show()
    // 6. 按年龄进行分组,并统计每个组的人数
    spark.sql("select age,count(*) from person_t group by age").show()

    println("--------------------------------------------")

    /**
     * DSL风格(以API的形式去执行SQL查询)
     * DSL的方法调用要遵循SQL的执行顺序,SQL的关键字执行顺序是:
     * 1. from
     * 2. where
     * 3. group by
     * 4. 聚合函数
     * 5. having
     * 6. select
     * 7. order by
     * 8. limit
     */
    // 1. 查看name字段
    ds.select($"name").show()

    // 2. 查看name和age字段
    ds.select($"name",$"age").show()

    // 3. 查询name和age字段,并将age结果+1
    ds.select($"name", $"age", ($"age"+1).as("age2")).show()

    // 4. 过滤年龄小于30岁
    ds.where($"age" <30).select($"id", $"name", $"age").show()

    // 5. 统计年龄小于30岁的人数
    val countResult: Long = ds.where($"age" < 30).count()
    println(s"统计年龄小于30岁的人数 = ${countResult}")

    // 6. 按年龄进行分组,并统计每个组的人数
    ds.groupBy($"age").count().show()

    sc.stop()
  }
}


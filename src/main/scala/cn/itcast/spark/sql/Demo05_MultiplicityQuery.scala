package cn.itcast.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
 * Author: itcast caoyu
 * Date: 2021-04-08 0008 20:01
 * Desc: 演示SparkSQL的一些基本查询API 以及DSL和SQL两种不同风格的写法
 */
object Demo05_MultiplicityQuery {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")


    // 先得到一个RDD[Person]
    val personRDD: RDD[Person] = sc.textFile("data/input/person.txt")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        Person(arr(0).toInt, arr(1), arr(2).toInt)
      })

    // 然后通过RDD得到一个DS
    import spark.implicits._
    val df: Dataset[Person] = personRDD.toDS()

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
    // 1. SQL风格需要先注册一个表(有4种方式)
    // SQL风格中表有全局和非全局
    // 全局表, 可以跨SparkSession使用(要使用全局表,需要在表名前面带上: global_temp.)
    // 非全局表, 只能在当前SparkSession使用
    df.createOrReplaceTempView("person")  // 创建或替换已有的 临时视图(表)
    /*
    df.createGlobalTempView()     // 仅创建一个 全局的表
    df.createOrReplaceGlobalTempView()  // 创建或替换  一个全局的表
    df.createTempView()           // 仅创建一个 表
     */
    // 2. 表注册好了就可以执行SQL
    // 1. 查看name字段
    spark.sql("SELECT name FROM person").show()
    // 2. 查看name和age字段
    spark.sql("SELECT name, age FROM person").show()
    // 3. 查询name和age字段,并将age结果+1
    spark.sql("SELECT name, age, (age + 1) AS age2 FROM person").show()
    // 4. 过滤年龄小于30岁
    spark.sql("SELECT * FROM person WHERE age < 30").show()
    // 5. 统计年龄小于30岁的人数
    spark.sql("SELECT COUNT(*) AS cnt FROM person WHERE age < 30").show()
    // 6. 按年龄进行分组,并统计每个组的人数
    spark.sql("SELECT age, COUNT(*) AS cnt FROM person GROUP BY age").show()

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
    df.select("name").show()              // 快捷方法,底层用的是标准用法
    df.select(df.col("name")).show()  // 标准用法
    df.select($"name").show() // 语法糖之一, 通过$列名 快速定位一个列
    df.select('name).show()   // 语法糖之一, 通过'列名 快速定位一个列
    import org.apache.spark.sql.functions._
    df.select(col("name")).show()  // 语法糖之一, 需要手动导隐式转换

    // 2. 查看name和age字段
    df.select($"name", $"age").show()

    // 3. 查询name和age字段,并将age结果+1
    df.select($"name", $"age", ($"age" + 1).as("age2")).show()
    // 4. 过滤年龄小于30岁
    df.where("age < 30").show()
    df.where($"age" < 30).show()
    df.filter($"age" < 30).show()
    df.filter(x => x.age < 30).show()
    df.select($"name", $"age", ($"age" + 1).as("age2"))
      .where("age < 30").show()
    // 5. 统计年龄小于30岁的人数
    println(s"年龄小于30岁的人数是: ${df.where("age < 30").count()}")
    // 6. 按年龄进行分组,并统计每个组的人数
    df.groupBy($"age").count().show()

    sc.stop()
  }
}

/**
 * 总结:
 * DSL风格 适合于复杂业务开发中使用
 * 因为和RDD计算一样,通过算子一步步迭代,逻辑是清晰的.
 * 在复杂业务计算中,写起来更简单. SQL可能写100行 嵌套一堆子查询. DSL可能十几个算子就搞定.
 *
 * SQL风格 适合于用于数仓开发,而且适用于大多数的人.
 * 可读性不好(想想5层嵌套的SQL 谁看得懂)
 */

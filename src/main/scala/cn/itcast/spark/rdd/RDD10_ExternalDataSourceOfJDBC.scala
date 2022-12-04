package cn.itcast.spark.rdd

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * Author: itcast caoyu
 * Date: 2021-03-30 0030 20:13
 * Desc: 演示从JDBC中读取数据
 */
object RDD10_ExternalDataSourceOfJDBC {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 准备一份数据写入到MySQL中
    val dataRDD: RDD[(Int, String)] = sc.makeRDD(List((1 -> "zhangsan"), (2 -> "lisi"), (3 -> "wangwu")))
    // 将RDD的数据写入到MySQL中
    /**
     * Spark 没有提供官方提供的MySQL写出API
     * 方法1:将RDD的数据全部获取到 通过标准JDBC的方式就能写出(这个不建议)
     * 方法2:或者,对RDD进行轮询, 将数据一批批的写出到MySQL中 (推荐)
     */
    // 这个collect方法,是将rdd的数据从各个分区中抽取出来,注入到一个Array对象中
    // 就是从分布式转单机的意思
    //    val array: Array[(Int, String)] = dataRDD.collect()


    // 如下写法是完全错误的.
    // 因为JDBC的连接构建在Driver执行,传递给executor用是不行的.
    // 因为序列化传输过去的只是对象的数据, 而无法将TCP连接传给executor用
    // TCP连接需要自己发起三次握手才行.

    // 构建JDBC环境
    //    val conn: Connection = DriverManager
    //      .getConnection(
    //        "jdbc:mysql://192.168.88.163:3306/test?characterEncoding=UTF-8"
    //        , "root"
    //        , "123456")
    //    val SQL_INSERT = """INSERT INTO person(id, name) VALUES(?, ?)"""
    //    // 批处理任务, 启动 -> 执行 -> 停止
    //    val ps: PreparedStatement = conn.prepareStatement(SQL_INSERT)
    //
    //    dataRDD.foreach(line => {
    //      ps.setInt(1, line._1)
    //      ps.setString(2, line._2)
    //      ps.execute()
    //    })
    //    conn.commit()


    // 这种写法是标准写法, 一个分区(不是一个executor)获取一个连接
    dataRDD.foreachPartition(iter => {
      val conn: Connection = DriverManager
        .getConnection(
          "jdbc:mysql://192.168.88.163:3306/test?characterEncoding=UTF-8"
          , "root"
          , "123456")
      val SQL_INSERT = """INSERT INTO person(id, name) VALUES(?, ?)"""
      // 批处理任务, 启动 -> 执行 -> 停止
      val ps: PreparedStatement = conn.prepareStatement(SQL_INSERT)
      iter.foreach(ele => {
        ps.setInt(1, ele._1)
        ps.setString(2, ele._2)
        ps.addBatch()
      })
      ps.executeBatch()
    })


    /**
     * 通过JDBC读取数据库的数据
     * 读取JDBC很好的是,Spark官方提供了API
     */
    // 本质上读取MySQL的数据,到Spark就是将数据读入到RDD中,也就是创建RDD的过程
    // Spark 提供了一个JdbcRDD的对象. 获得这个对象即可.
    case class Person(id:Int, name:String)

    val personRDD = new JdbcRDD[Person](
      sc,
      // 传入这个函数, 让每个executor自行构建到MySQL的链接.
      () => DriverManager.getConnection(
        "jdbc:mysql://192.168.88.163:3306/test?characterEncoding=UTF-8"
        , "root"
        , "123456")
      // 源码要求,必须有?占位符,用来确定查询边界
      , "SELECT * FROM person WHERE id>=? AND id <= ?",
      1,
      3,
      1,
      (rs: ResultSet) => Person(rs.getInt("id"), rs.getString("name"))
    )

    personRDD.foreach(println)


// id>=? AND id <= ?



    // 这个不用纠结
    // MySQL能存多少数据?  1G表
    // Spark是处理什么量级的框架 ? PB级
     // OLAP : 查出来(不带逻辑的查), 写进去(不带逻辑的写)
    // 10分钟
  }
}

package cn.itcast.test.rdd

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
    // 初始sc
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 准备数据
    val rdd: RDD[(Int, String)] = sc.parallelize(List((1, "张1"), (2, "李2"), (3, "王3"), (4, "赵4")))

    // 插入数据
    rdd.foreachPartition(iter =>{
      val conn: Connection = DriverManager.getConnection(
        "jdbc:mysql://192.168.88.163:3306/mytest?characterEncoding=UTF-8",
        "root",
        "123456")
      val insertSql = "insert into person values(?,?)"
      val ps: PreparedStatement = conn.prepareStatement(insertSql)
      iter.foreach(ele =>{
        ps.setInt(1, ele._1)
        ps.setString(2, ele._2)
        ps.addBatch()
      })
      ps.executeBatch()
      ps.close()
      conn.close()
    })

    // 查询数据
    val personRDD = new JdbcRDD[Person](
      sc,
      () => DriverManager.getConnection(
        "jdbc:mysql://192.168.88.163:3306/mytest?characterEncoding=UTF-8",
        "root",
        "123456"),
      "select * from person where id>=? and id<=?",
      1,
      4,
      1,
      (rs: ResultSet) => {
        Person(rs.getInt("id"), rs.getString("name"))
      }
    )

    personRDD.foreach(println(_))

    sc.stop()
  }

  case class Person(id:Int, name:String)
}

package cn.itcast.spark.sql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * SparkSQL 启动ThriftServer服务，通过JDBC方式访问数据分析查询
 */
object Demo13_SparkThriftServerJDBC {
  def main(args: Array[String]): Unit = {
    // 定义相关实例对象，未进行初始化
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try {
      // TODO： a. 加载驱动类
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      // TODO: b. 获取连接Connection
      conn = DriverManager.getConnection(
        "jdbc:hive2://node1:10000/default",
        "root",
        "123456"
      )
      // TODO: c. 构建查询语句
      val sqlStr: String =
        """
          |select * from person
                """.stripMargin
      ps = conn.prepareStatement(sqlStr)
      // TODO: d. 执行查询，获取结果
      rs = ps.executeQuery()
      // 打印查询结果
      while (rs.next()) {
        println(s"id = ${rs.getInt(1)}, name = ${rs.getString(2)}, age = ${rs.getInt(3)}}")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != rs) rs.close()
      if (null != ps) ps.close()
      if (null != conn) conn.close()
    }
  }
}
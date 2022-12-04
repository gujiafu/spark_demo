package cn.itcast.spark.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author: itcast caoyu
 * Date: 2021-03-30 0030 21:26
 * Desc: 演示Spark读取HBase数据
 */
object RDD11_SparkReadHBase {
  def main(args: Array[String]): Unit = {
    // 0. Init Env
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    /**
     * MapReduce代码读取HBase用的叫:TableInputFormat类
     * Spark支持调用HadoopAPI类,我们用TableInputFormat类
     * 就可以让Spark读取HBase数据
     */
    // 构建Hadoop的配置对象
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node1")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    // 设置读取的表的名称
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "htb_wordcount")

    // Spark提供的调用HadoopAPI的方法
    val resultRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], // 完成HBase key 的序列化
      classOf[Result] // HBase的查询结果是Result对象
    )

    println(s"查出来HBase的条目数量: ${resultRDD.count()}")
    resultRDD.foreach{
      case (x:ImmutableBytesWritable, result:Result) => {
        result.rawCells().foreach((cell:Cell) => {
          val rowkey = Bytes.toString(CellUtil.cloneRow(cell))
          val family = Bytes.toString(CellUtil.cloneFamily(cell))
          val column = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          val ts = cell.getTimestamp

          println(s"rowkey:$rowkey, family:$family, column:$column, value:$value, ts:$ts")
        })
      }
    }
  }
}

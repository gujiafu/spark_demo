package cn.itcast.spark.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 部署到 standalone 运行
 ./bin/spark-submit \
  --class cn.itcast.spark.hello.WordCountStandAlone \
  --master spark://node1:7077 \
  --executor-memory 2G \
  --total-executor-cores 3 \
  /export/data/spark_demo-1.0-SNAPSHOT.jar  \
  1000
 */
object WordCountStandAlone {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("yarn作业").setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")
    val rdd: RDD[String] = context.textFile("hdfs://node1:8020/wordcount/input/words.txt")
    rdd.flatMap(_.split(" "))
      .map(x => (x,1))
      .reduceByKey((x,y) => x+y)
      .foreach(x => println(x))

  }
}

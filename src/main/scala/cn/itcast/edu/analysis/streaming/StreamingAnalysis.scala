package cn.itcast.edu.analysis.streaming

import cn.itcast.edu.bean.Answer
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * 实时分析学生答题情况
 *
实时业务1: 实时统计Top10热点题
实时业务2: 实时统计答题最活跃的Top10年级
实时业务3: 实时统计每个科目的Top10热点题
实时业务4: 实时统计每位学生得分最低的题目Top10
 */
object StreamingAnalysis {
  def main(args: Array[String]): Unit = {

    // 初始环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    // 隐式依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 输入
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092,")
      .option("subscribe", "edu")
      .option("startingOffsets", "earliest")
      .load()

    // 转换
    val answerDS: Dataset[Answer] = kafkaDF.selectExpr("CAST(value as string)")
      .as[String]
      .filter(StringUtils.isNoneBlank(_))
      .map(JSON.parseObject(_, classOf[Answer]))
    answerDS.createOrReplaceTempView("answer_t")

    // 实时业务1: 实时统计Top10热点题
    val result01DF: DataFrame = spark.sql(
      """
        |select
        |question_id,
        |count(1) as cnt
        |from answer_t
        |group by question_id
        |order by cnt desc
        |limit 10
        |""".stripMargin)

    // 实时业务2: 实时统计答题最活跃的Top10年级
    val result02DF: DataFrame = spark.sql(
      """
        |select
        |grade_id,
        |count(1) as cnt
        |from answer_t
        |group by grade_id
        |order by cnt desc
        |limit 10
        |""".stripMargin)

    // 实时业务3: 实时统计每个科目的Top10热点题
    val result03DF: DataFrame = spark.sql(
      """
        |select
        |subject_id,
        |question_id,
        |count(1) as cnt
        |from answer_t
        |group by subject_id, question_id
        |order by cnt desc
        |limit 10
        |""".stripMargin)

    // 实时业务4: 实时统计每位学生得分最低的题目Top10
    val result04DF: DataFrame = spark.sql(
      """SELECT
        |  student_id, FIRST(question_id), MIN(score)
        |FROM
        |  t_answer
        |GROUP BY
        |  student_id
        |order by
        |  score
        |limit 10
        |""".stripMargin)


    // 输出
    result01DF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", false)
      .start()

    result02DF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", false)
      .start()

    result03DF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", false)
      .start()

    result04DF.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate", false)
      .start()

    spark.streams.awaitAnyTermination()
    sc.stop()
  }
}

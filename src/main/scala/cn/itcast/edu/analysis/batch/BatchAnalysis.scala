package cn.itcast.edu.analysis.batch


import cn.itcast.edu.bean.AnswerWithRecommendations
import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 使用SparkSQL对学生学习情况/答题情况做离线分析
 */
object BatchAnalysis {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark: SparkSession = SparkSession.builder().appName("BatchAnalysis")
      .master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4") //默认是200,本地测试给少一点
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN") //注意日志级别:debug、info、warn、error，设置为warn表示只显示warn和error

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //2.加载数据
    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    val allInfoDS: Dataset[AnswerWithRecommendations] = spark.read.jdbc(
      "jdbc:mysql://localhost:3306/edu?useUnicode=true&characterEncoding=utf8",
      "t_recommended",
      properties
    ).as[AnswerWithRecommendations]

    //3.业务实现:
    //TODO 需求1:各科目热点题分析
    // 要求: 找到Top50热点题对应的科目，然后统计这些科目中，分别包含这几道热点题的条目数
    /*
    热点题
    题号 热度 学科
    1   100  数学
    2   99   数学
    3   98   语文

    最终结果:
    学科  热点题数量
    数学  2
    语文 1
     */
    //方式1:SQL风格
    allInfoDS.createTempView("t_answer")
    spark.sql(
      """SELECT
        |  subject_id, count(t_answer.question_id) AS hot_question_count
        | FROM
        |  (SELECT
        |    question_id, count(*) AS frequency
        |  FROM
        |    t_answer
        |  GROUP BY
        |    question_id
        |  ORDER BY
        |    frequency
        |  DESC LIMIT
        |    50) t1
        |JOIN
        |  t_answer
        |ON
        |  t1.question_id = t_answer.question_id
        |GROUP BY
        |  subject_id
        |ORDER BY
        |  hot_question_count
        | DESC
  """.stripMargin)
      .show()

    //方式2:DSL风格
    //-1.Top50热点题
    //top50DS: Dataset[Row(题目id,题目热度)]
    val top50DS: Dataset[Row] = allInfoDS.groupBy('question_id)
      .count()
      .orderBy('count.desc)
      .limit(50)
    //-2.和原始数据进行管理,得到题目所属学科,热点题数量
    //join的时候可以写表达式或指定使用哪个列进行join
    //df1.join(df2, $"df1Key" === $"df2Key")--表达式
    //df1.join(df2).where($"df1Key" === $"df2Key")--表达式
    //df1.join(df2, "user_id")--指定使用哪个列进行join
    top50DS.join(allInfoDS, "question_id") //题目id,题目热度,学科id
      .groupBy('subject_id)
      .count()
      .orderBy('count.desc)
      .show(false)


    //TODO 需求2:各科目推荐题分析
    // 要求: 找到Top20热点题对应的推荐题目，然后找到推荐题目对应的科目，并统计每个科目分别包含推荐题目的条数
    /*
    热点题对应的学科和推荐题号
    题号 热度 学科  推荐题号
    1    100  数学   234
    2    99   数学   345
    3    98   语文   678

    最终结果
    学科   推荐题数量
    数学     4道
    语文     3道

    复杂一点:有可能推荐的题不是同一个学科的
    题号 热度   推荐题号
    1    100     234
    2    99      345
    3    98      678
    推荐题号 对应学科
     2        数学
     3        语文
     4        数学
     5        数学
     6        数学
     7        语文
     8        语文
    最终结果
    学科  推荐的热点题数量
    数学     4
    语文     3
     */

    //方式1:SQL方式
    spark.sql(
      """SELECT
        |    t4.subject_id,
        |    COUNT(*) AS frequency
        |FROM
        |    (SELECT
        |        DISTINCT(t3.question_id),
        |        t_answer.subject_id
        |     FROM
        |       (SELECT
        |           EXPLODE(SPLIT(t2.recommendations, ',')) AS question_id
        |        FROM
        |            (SELECT
        |                recommendations
        |             FROM
        |                 (SELECT
        |                      question_id,
        |                      COUNT(*) AS frequency
        |                  FROM
        |                      t_answer
        |                  GROUP BY
        |                      question_id
        |                  ORDER BY
        |                      frequency
        |                  DESC LIMIT
        |                      20) t1
        |             JOIN
        |                 t_answer
        |             ON
        |                 t1.question_id = t_answer.question_id) t2) t3
        |      JOIN
        |         t_answer
        |      ON
        |         t3.question_id = t_answer.question_id) t4
        |GROUP BY
        |    t4.subject_id
        |ORDER BY
        |    frequency
        |DESC
  """.stripMargin).show

    //方式2:DSL风格
    //1.统计热点题Top20--子查询t1
    //top20DS[题目id,题目热度]
    val top20DS: Dataset[Row] = allInfoDS.groupBy('question_id)
      .count()
      .orderBy('count.desc)
      .limit(20)

    //2.将t1和原始表t_answer关联,得到热点题Top20的推荐题列表t2
    //题目id,推荐列表
    //recommendListDF[题目id,推荐列表题目ids字符串]
    //如:[题目ID_999,推荐列表ids字符串:题目ID_44,题目ID_232,题目ID_2118,题目ID_1265]
    val recommendListDF: DataFrame = top20DS.join(allInfoDS,"question_id")

    //3.用SPLIT将recommendations中的ids用","切为数组,然后用EXPLODE将列转行,并记为t3
    //将上面的数据推荐列表ids字符串:
    // 题目ID_44,题目ID_232,题目ID_2118,题目ID_1265
    //变为:
    //题目id
    //题目ID_44
    //题目ID_232
    //题目ID_2118
    //题目ID_1265
    //先用,号切割,再用explod将一行变为多行
    //Creates a new row for each element in the given array or map column.
    //为给定数组或映射列中的每个元素创建新行。
    val questionIdDF: DataFrame = recommendListDF.select(explode(split('recommendations,",")).as("question_id"))

    //4.对推荐的题目进行去重,将t3和t_answer原始表进行join,得到每个推荐的题目所属的科目,记为t4
    //questionIdAndSubjectIdDF[推荐的题号,学科]
    val questionIdAndSubjectIdDF: DataFrame = questionIdDF.distinct()
      .join(allInfoDS.dropDuplicates("question_id"), "question_id") //实际中和题号-学科字典表进行join即可,我们这里和allInfoDS进行join,allInfoDS里面有需要去重
      .select('question_id, 'subject_id)

    //5.统计各个科目包含的推荐的题目数量并倒序排序(已去重)
    questionIdAndSubjectIdDF
      .groupBy('subject_id)
      .count()
      .orderBy('count.desc).show(false)
    /*
    +-------------+-----+
    |subject_id   |count|
    +-------------+-----+
    |科目ID_3_英语|106  |
    |科目ID_1_数学|91   |
    |科目ID_2_语文|69   |
    +-------------+-----+
     */
  }
}
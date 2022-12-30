package cn.itcast.edu.analysis.streaming


import cn.itcast.edu.bean.{Answer, AnswerWithRecommendations}
import cn.itcast.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

/**
 * Author itcast
 * Desc 使用SparkStreaming+ALS模型(已经训练好了,可以直接使用)完成实时易错题推荐
 */
object StreamingRecommend {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark: SparkSession = SparkSession.builder().appName("StreamingRecommend").master("local[*]")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "4") //默认是200,本地测试给少一点
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //2.从Kafka实时获取数据
    //spark.readStream.format("kafka").option().....
    val topics = Array("edu")
    val kafkaParams = Map(
      "bootstrap.servers" -> "node1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkStreaming",
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms" -> "1000", //自动提交的时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //3.获取kafkaDStream中的value(json字符串)并转为样例类
    val AnswerDStream: DStream[Answer] = kafkaDStream.map(record => {
      val jsonStr: String = record.value()
      val gson = new Gson()
      gson.fromJson(jsonStr, classOf[Answer])
    })

    //4.遍历数据,给每个用户实时推荐易错题的id
    AnswerDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        /*rdd.foreachPartition(iter => {
          //-0.获取redis连接
          val jedis: Jedis = RedisUtil.pool.getResource
          //-2.加载ALS模型path
          //jedis.hset("als_model", "recommended_question_id", path)
          val path: String = jedis.hget("als_model", "recommended_question_id")
          //3.-根据path加载训练好的模型
          val model: ALSModel = ALSModel.load(path)
          iter.foreach(answer=>{
            //4.根据模型给用户推荐易错题
            //model.recommendForUserSubset(Dataset,Int)
          })
          //将redis连接归还到池子中
          jedis.close()
        })*/
        //-0.获取redis连接
        val jedis: Jedis = RedisUtil.pool.getResource
        //-2.加载ALS模型path
        //jedis.hset("als_model", "recommended_question_id", path)
        val path: String = jedis.hget("als_model", "recommended_question_id")
        //-3.根据path加载训练好的模型
        val model: ALSModel = ALSModel.load(path)
        //-4.将RDD转为DF方便后面的参数传递//.coalesce(1)保证Redis连接只获取一次//因为DF没有foreachPartition方法
        val answerDF: DataFrame = rdd.toDF.coalesce(1)
        //-5.取出参数中需要的用户id/学生id(去掉前缀)
        val id2Int = udf((id: String) => {
          id.split("_")(1).toInt
        })
        val userDF: DataFrame = answerDF.select(id2Int('student_id) as "student_id")
        //-6.使用加载出来的训练好的ALS模型给当前批次的用户子集,推荐20道易错题
        val recommendDF: DataFrame = model.recommendForUserSubset(userDF, 20)
        recommendDF.printSchema()
        /*
        student_id,recommend推荐的题的ids
        student_id,recommendations[(question_id,rating)]
        root
         |-- student_id: integer (nullable = false)
         |-- recommendations: array (nullable = true)
         |    |-- element: struct (containsNull = true)
         |    |    |-- question_id: integer (nullable = true)
         |    |    |-- rating: float (nullable = true)//评分/推荐指数
         */
        //-7.取出student_id和recommendations中的question_id
        val transRecommendDF: DataFrame = recommendDF.as[(Int, Array[(Int, Float)])].map(row => {
          val recommendationIds: ArrayBuffer[String] = ArrayBuffer[String]() //用来存放推荐的题的Id
          val id: Int = row._1
          val arr: Array[(Int, Float)] = row._2
          for (i <- arr) {
            val recommendation_question_id: String = "题目ID_" + i._1
            recommendationIds += recommendation_question_id
          }
          val student_id: String = "学生ID_" + id
          //(学生id, 推荐的题的ids)
          (student_id, recommendationIds.mkString(","))
        }).toDF("student_id", "recommendations")
        transRecommendDF.show(false)
        //或者使用如下转换
        val transRecommendDF2: DataFrame = recommendDF.map(row => {
          val recommendationIds: ArrayBuffer[String] = ArrayBuffer[String]() //用来存放推荐的题的Id
          val id = row.getInt(0)
          val arr: Seq[GenericRowWithSchema] = row.getSeq[GenericRowWithSchema](1)
          for (i <- arr) {
            val recommendation_question_id = "题目ID_" + i.getInt(0)
            recommendationIds += recommendation_question_id
          }
          val student_id: String = "学生ID_" + id
          //(学生id, 推荐的题的ids)
          (student_id, recommendationIds.mkString(","))
        }).toDF("student_id", "recommendations")

        //-8.将推荐结果和原始数据df[Answer]做关联,得到原始信息Answer+推荐的题目id==>AnswerWithRecommendations
        val allInfoDS: Dataset[AnswerWithRecommendations] = transRecommendDF.join(answerDF, "student_id")
          .as[AnswerWithRecommendations].coalesce(1)
        allInfoDS.show()
        val count: Long = allInfoDS.count()

        if (count > 0) {
          //-9.推荐结果存入MySQL
          val properties = new java.util.Properties()
          properties.setProperty("user", "root")
          properties.setProperty("password", "root")
          allInfoDS.write.mode(SaveMode.Append).jdbc(
            "jdbc:mysql://localhost:3306/edu?useUnicode=true&characterEncoding=utf8",
            "t_recommended",
            properties)
        }
        //-将redis连接归还到池子中
        jedis.close()
      }
    })
    //5.启动程序
    ssc.start
    ssc.awaitTermination()
  }
}
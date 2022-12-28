package cn.itcast.edu.mock


import java.util.concurrent.{ArrayBlockingQueue, Executors, ThreadPoolExecutor, TimeUnit}


/**
 * 使用线程池调度Kafka生产者发送任务,将数据实时发送到Kafka
 */
object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    //创建线程池
    val threadPoolExecutor: ThreadPoolExecutor = new ThreadPoolExecutor(5, //活跃线程数
      10, //最大线程数
      5, //最大空闲时间
      TimeUnit.SECONDS, //时间单位
      new ArrayBlockingQueue[Runnable](10))//任务等待队列,未被调度的线程任务,会在该队列中排队
    //提交任务
    for (i <- 1 to 4) {
      threadPoolExecutor.submit(new KafkaProducerThread)
    }
  }
}
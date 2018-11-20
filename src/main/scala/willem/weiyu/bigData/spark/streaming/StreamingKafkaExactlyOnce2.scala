package willem.weiyu.bigData.spark.streaming

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import willem.weiyu.bigData.spark.common.ZookeeperOffsetManager

/**
  * @author weiyu
  * @description direct方式连接kafka，使用zookeeper存储offset以实现exactly-once
  * @Date 2018/10/31 16:35
  */
object StreamingKafkaExactlyOnce2 {
  val MASTER = "local[4]"
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val TOPIC = "test"
  val GROUP_ID = "weiyu"
  val BATCH_DURATION = 5
  val ZK_HOSTS = "10.26.27.81:2181"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    //开启背压,通过spark.streaming.kafka.maxRatePerPartition限制速率（默认为false）
    //conf.set("spark.streaming.backpressure.enabled","true")
    //确保在kill任务时，能够处理完最后一批数据再关闭程序（默认为false）
    //conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //限制第一次批处理应消费的数据，防止造成系统阻塞（默认读取所有）
    //conf.set("spark.streaming.backpressure.initialRate","1000")
    //限制每秒每个消费线程读取每个kafka分区最大的数据量
    //conf.set("spark.streaming.kafka.maxRatePerPartition","1000")
    val ssc = new StreamingContext(conf,Seconds(BATCH_DURATION))
    //ssc.checkpoint(CHECKPOINT_PATH)
    val kafkaParams = Map("group.id"->GROUP_ID,//消费者组
      "bootstrap.servers"->"10.26.27.81:9092",//kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),//为true时，偏移量会在后台自动提交，设为false自己提交offset（保证数据处理完成后才提交offset）
      "auto.offset.reset"->"earliest")//当各分区下有已提交的offset时，从提交的offset开始消费，无提交的从指定位置（earliest,latest）开始消费
    val topics = Seq(TOPIC)

    val zookeeperOffsetManager = new ZookeeperOffsetManager(ZK_HOSTS, kafkaParams)

    val stream = zookeeperOffsetManager.createDirectStream[String, String](ssc, topics, kafkaParams)

    val result = stream.map(record => {
      val obj = JSON.parseObject(record.value())
      val uid = obj.getString("uid")
      val clickCount = obj.getIntValue("click_count")
      (uid,clickCount)
    }).reduceByKey(_+_)

    result.print(10)

    zookeeperOffsetManager.persistOffset(stream, true)

    ssc.start()
    ssc.awaitTermination()
  }
}
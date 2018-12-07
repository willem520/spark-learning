package willem.weiyu.bigData.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * @author weiyu
  * @description direct方式连接kafka，使用kafka内部的特殊topic：__consumer_offsets中存储offset以实现exactly-once
  *             可通过offsets.retention.minutes自定义offset过期时间
  * @Date 2018/10/31 16:35
  */
object StreamingKafkaExactlyOnce {
  val MASTER = "local[4]"
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val TOPIC = "test"
  val GROUP_ID = "weiyu"
  val BATCH_DURATION = 5

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    val sparkConf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    //开启优雅关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //开启背压,通过spark.streaming.kafka.maxRatePerPartition限制速率（默认为false）
    sparkConf.set("spark.streaming.backpressure.enabled","true")
    //确保在kill任务时，能够处理完最后一批数据再关闭程序（默认为false）
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    //限制第一次批处理应消费的数据，防止造成系统阻塞（默认读取所有）
    sparkConf.set("spark.streaming.backpressure.initialRate","1000")
    //限制每秒每个消费线程读取每个kafka分区最大的数据量
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")

    val ssc = StreamingContext.getOrCreate(CHECKPOINT_PATH, ()=>getStreamingContext(sparkConf,BATCH_DURATION,CHECKPOINT_PATH))

    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(sparkConf: SparkConf, duration : Int, checkpointDir : String): StreamingContext = {
    val ssc = new StreamingContext(sparkConf,Seconds(BATCH_DURATION))

    ssc.checkpoint(CHECKPOINT_PATH)
    val kafkaParams = Map("group.id"->GROUP_ID,//消费者组
      "bootstrap.servers"->"10.26.27.81:9092",//kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),//为true时，偏移量会在后台自动提交，设为false自己提交offset（保证数据处理完成后才提交offset）
      "auto.offset.reset"->"earliest")//当各分区下有已提交的offset时，从提交的offset开始消费，无提交的从指定位置（earliest,latest）开始消费
    val topics = Set(TOPIC)

    val stream = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String, String](topics,kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition{ partitionOfRecords =>
        val offset = offsetRanges(TaskContext.get.partitionId)
        //处理逻辑
        println(s"The record from topic [${offset.topic}] is in partition ${offset.partition} which offset from ${offset.fromOffset} to ${offset.untilOffset}")
        println(s"The record content is ${partitionOfRecords.toList.mkString}")
      }
      //等操作完成后再提交offset
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc
  }
}

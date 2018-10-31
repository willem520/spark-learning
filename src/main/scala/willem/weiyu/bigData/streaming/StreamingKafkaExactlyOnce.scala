package willem.weiyu.bigData.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * @author weiyu
  * @description direct方式连接kafka，使用kafka内部的特殊topic：__consumer_offsets中存储offset以实现exactly-once
  * @Date 2018/10/31 16:35
  */
object StreamingKafkaExactlyOnce {
  val TOPIC = "test"
  val GROUP_ID = "weiyu"
  val CHECKPOINT_PATH = "/spark/checkpoint"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("kafkaDemo")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint(CHECKPOINT_PATH)
    val kafkaParams = Map("group.id"->GROUP_ID,
      "zookeeper.connect" -> "10.26.27.81:2181",
      "bootstrap.servers"->"10.26.27.81:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset"->"earliest")
    val topics = Set(TOPIC)

    val stream = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String, String](topics,kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition{ item =>
        val offset = offsetRanges(TaskContext.get.partitionId)
        println(s"The record from topic [${offset.topic}] is in partition ${offset.partition} which offset from ${offset.fromOffset} to ${offset.untilOffset}")
        println(s"The record content is ${item.toList.mkString}")
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

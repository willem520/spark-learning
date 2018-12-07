package willem.weiyu.bigData.spark.common

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

/**
  * @Author weiyu005
  * @Description 管理存储在zookeeper上的kafka offset
  * @Date 2018/11/19 12:26
  */
class KafkaOffsetManager(zkHosts: String, kafkaParams: Map[String, Object]) extends Serializable {
  @transient private lazy val log = LoggerFactory.getLogger(getClass)
  val ZK_SESSION_TIMEOUT = 3000
  val ZK_CONNECTION_TIMEOUT = 3000

  val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkHosts, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT)
  val zookeeperUtils = new ZkUtils(zkClient, zkConnection, false)


  /**
    * 包装createDirectStream方法，支持Kafka Offset，用于创建Kafka Streaming流
    * @param ssc
    * @param topics
    * @tparam K
    * @tparam V
    * @return
    */
  def createDirectStream[K: ClassTag, V: ClassTag](ssc: StreamingContext, topics: Seq[String], kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[K, V]] = {
    val groupId = kafkaParams("group.id").toString
    val storedOffsets = readOffsets(topics, groupId)
    log.info("Kafka消息偏移量汇总(格式:(话题,分区号,偏移量)):{}", storedOffsets.map(off => (off._1.topic, off._1.partition(), off._2)))
    val kafkaStream = KafkaUtils.createDirectStream[K, V](ssc, PreferConsistent, Subscribe[K, V](topics, kafkaParams, storedOffsets))
    kafkaStream
  }

  /**
    * 从zookeeper读取kafka消息队列的offset
    * @param topics
    * @param groupId
    * @return
    */
  def readOffsets(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zookeeperUtils.getPartitionsForTopics(topics)

    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition =>{
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition
        val tryGetKafkaOffset = Try {
          val offsetStatTuple = zookeeperUtils.readData(offsetPath)
          if (offsetStatTuple != null){
            log.info("查询Kafka消息偏移量详情: 话题:{}, 分区:{}, 偏移量:{}, ZK节点路径:{}", Seq[AnyRef](topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath): _*)
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), offsetStatTuple._1.toLong)
          }
        }
        if (tryGetKafkaOffset.isFailure){
          val consumer = new KafkaConsumer[String, Object](kafkaParams)
          val partitionList = List(new TopicPartition(topicPartitions._1, partition))
          consumer.assign(partitionList)
          val minAvailableOffset = consumer.beginningOffsets(partitionList).values.head
          consumer.close
          log.warn("查询Kafka消息偏移量详情: 没有上一次的ZK节点:{}, 话题:{}, 分区:{}, ZK节点路径:{}, 使用最小可用偏移量:{}",
            Seq[AnyRef](tryGetKafkaOffset.failed.get.getMessage, topicPartitions._1, partition.toString, offsetPath, minAvailableOffset): _*)
          topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), minAvailableOffset)
        }
      })
    })
    topicPartOffsetMap.toMap
  }

  /**
    * 保存kafka offset
    * @param rdd
    * @param storeEndOffset
    * @tparam K
    * @tparam V
    */
  def persistOffset[K, V](rdd: InputDStream[ConsumerRecord[K, V]], storeEndOffset: Boolean = true): Unit = {
    val groupId = kafkaParams("group.id").toString
    rdd.foreachRDD(r =>{
      val offsetRanges = r.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(or => {
        val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition
        val offsetVal = if(storeEndOffset) or.untilOffset else or.fromOffset
        zookeeperUtils.updatePersistentPath(offsetPath, offsetVal.toString)
        log.info("保存Kafka消息偏移量详情: 话题:{}, 分区:{}, 偏移量:{}, ZK节点路径:{}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)
      })
    })
  }
}
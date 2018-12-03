package willem.weiyu.bigData.spark.streaming

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.bson.Document
import willem.weiyu.bigData.spark.common.MongoPool

/**
  * @Author weiyu005
  * @Description
  * @Date 2018/12/03 18:16
  */
object Streaming2Mongo {
  val MASTER = "local[*]"
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val TOPIC = "test"
  val GROUP_ID = "weiyu"
  val BATCH_DURATION = 5
  val BOOTSTRAP_SERVERS = "10.26.27.81:9092"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Seconds(BATCH_DURATION))
    ssc.checkpoint(CHECKPOINT_PATH)

    val kafkaParams = Map("group.id"->GROUP_ID,
      "bootstrap.servers"-> BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset"->"earliest")
    val topics = Set(TOPIC)

    val stream = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String, String](topics,kafkaParams))

    val event = stream.flatMap(record => {
      val data = JSON.parseObject(record.value())
      Some(data)
    })

    val userClicks = event.map(x => (x.getString("uid"), x.getIntValue("click_count"))).reduceByKey(_+_)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(records=>{
        val nodes = "10.26.15.199:27017"
        lazy val  client = MongoPool(nodes)
        lazy val  coll = client.getDatabase("streaming").getCollection("page_view")
        records.foreach(r =>{
          coll.insertOne(new Document().append("uid",r._1).append("clicks",r._2))
        })
      })
    })

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition{ item =>
        val offset = offsetRanges(TaskContext.get.partitionId)
        println(s"The record from topic [${offset.topic}] is in partition ${offset.partition} which offset from ${offset.fromOffset} to ${offset.untilOffset}")
        println(s"The record content is ${item.toList.mkString}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

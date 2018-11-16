package willem.weiyu.bigData.spark.streaming

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * @Author weiyu005@ke.com
  * @Description
  * @Date 2018/11/15 11:45
  */
object Streaming2HBase {
  val MASTER = "local[4]"
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
        val tableName = "pageViewStream"
        val hbaseConf = HBaseConfiguration.create()
        /*hbaseConf.set("hbase.zookeeper.quorum", "10.26.27.81")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("hbase.defaults.for.version.skip", "true")*/
        val conn = ConnectionFactory.createConnection(hbaseConf)
        val table = conn.getTable(TableName.valueOf(tableName))
        records.foreach(pair =>{
          val uid = pair._1
          val click = pair._2
          val put = new Put(Bytes.toBytes(uid))
          put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("clickStat"), Bytes.toBytes(click))
          table.put(put)
        })
        table.close
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

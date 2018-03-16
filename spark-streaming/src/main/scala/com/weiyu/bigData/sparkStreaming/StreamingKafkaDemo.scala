package com.weiyu.bigData.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu@gomeholdings.com
  */
object StreamingKafkaDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("kafkaDemo")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("check")
    val kafkaParams = Map("zookeeper.connect" -> "10.143.90.38:2181,10.143.90.39:2181,10.143.90.49:2181",
      "group.id"->"weiyu-test","bootstrap.servers"->"10.143.90.38:9092,10.143.90.39:9092,10.143.90.49:9092",
      "auto.offset.reset"->"smallest")
    val topic = Set("realtime_test")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topic)

    //过滤错误信息
    stream.map(_._2).filter(x => x.contains("Error") || x.contains("Fatal")).flatMap(_.split("-").head).print(10)

    stream.map(_._2).flatMap(_.split("-").head).print(20)

    ssc.start()
    ssc.awaitTermination()
  }
}

package willem.weiyu.bigData.spark.sink

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @Author weiyu005
  * @Description
  * @Date 2018/12/7 14:45
  */
class KafkaSink(createProducer:()=> KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic:String, key:String, value:String): Unit ={
    producer.send(new ProducerRecord(topic, key, value))
  }

  def send(topic: String, partition: Int, key: String, value: String): Unit ={
    producer.send(new ProducerRecord(topic, partition, key, value))
  }
}

object KafkaSink {

  def apply(config: Properties): KafkaSink = {
    new KafkaSink(()=>{
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook{
        producer.close()
      }
      producer
    })
  }
}
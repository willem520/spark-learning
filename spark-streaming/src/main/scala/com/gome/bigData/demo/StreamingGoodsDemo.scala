package com.gome.bigData.demo


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingGoodsDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streamingGoodsDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("check")
    val line = ssc.socketTextStream("127.0.0.1",9999)

    val result = line.flatMap(x=>{
      val goodMsg = x.split("::")
      val followValue = goodMsg(1).toDouble*0.8+goodMsg(2).toDouble*0.6+goodMsg(3).toDouble*1+goodMsg(4).toDouble*1
      Some(goodMsg(0),followValue.formatted("%.2f"))
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

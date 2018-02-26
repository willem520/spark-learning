package com.gome.bigData.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu@gomeholdings.com
  */
object StreamingDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("streamingDemo")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("check")
    val lines = ssc.socketTextStream("10.143.103.26", 9999)

    /**
      * 按空格分隔
      */
    val words = lines.flatMap(_.split("\\s"))

    /**
      * 单个word变成tuple,并累加
      */
    /*val wordCount = words.map(word =>(word, 1)).reduceByKey(_+_)*/

    val func = (oldVal:Seq[Int],newVal:Option[Int])=>{
      Some(oldVal.sum + newVal.getOrElse(0))
    }

    val wordCount = words.map(word =>(word, 1)).updateStateByKey[Int](func)

    wordCount.print()
    ssc.start()

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}

package willem.weiyu.bigData.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 无状态单词统计
  * @Date 2018/11/05 18:20
  */
object StreamingDemo {
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val DURATION = 5
  //  val HOST = "localhost"
  val HOST = "10.26.27.81"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("streamingDemo")
    val ssc = new StreamingContext(conf,Seconds(5))
    val lines = ssc.socketTextStream(HOST, 9999)

    /**
      * 按空格分隔
      */
    val words = lines.flatMap(_.split(",|，|\\s+"))

    /**
      * 单个word变成tuple
      */
    val wordCount = words.map((_, 1)).reduceByKey(_+_)

    wordCount.print()
    ssc.start()

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}

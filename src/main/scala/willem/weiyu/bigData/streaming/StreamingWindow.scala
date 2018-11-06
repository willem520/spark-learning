package willem.weiyu.bigData.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 窗口统计
  * @Date 2018/11/05 18:20
  */
object StreamingWindow {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("streamingDemo")
    val ssc = new StreamingContext(conf,Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)

    /**
      * 按空格分隔
      */
    val words = lines.flatMap(_.split(",|，|\\s+"))

    val wordCount = words.map((_, 1)).reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))

    wordCount.print()
    ssc.start()

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}

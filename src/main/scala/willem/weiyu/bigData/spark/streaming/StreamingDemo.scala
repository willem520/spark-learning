package willem.weiyu.bigData.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 无状态单词统计
  * @Date 2018/11/05 18:20
  */
object StreamingDemo {
  val MASTER = "local[4]"
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val BATCH_DURATION = 5
  val HOST = "localhost"
//  val HOST = "10.26.27.81"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Seconds(BATCH_DURATION))

    val lines = ssc.socketTextStream(HOST, 9999)

    /**
      * 根据正则分隔
      */
    val words = lines.flatMap(_.split(",|，|\\s+"))

    /**
      * 单个word变成tuple
      */
    val wordCount = words.map((_, 1)).reduceByKey(_+_)
    wordCount.print()

    /**
      * 程序启动
      */
    ssc.start()

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}

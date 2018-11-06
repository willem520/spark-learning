package willem.weiyu.bigData.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 将窗口中元素进行reduce
  * @Date 2018/11/05 18:20
  */
object StreamingReduceByWindow {
  val CHECKPOINT_PATH = "/spark/checkpoint"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("streamingDemo")
    val ssc = new StreamingContext(conf,Seconds(1))
    ssc.checkpoint(CHECKPOINT_PATH)
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(",|，|\\s+"))

    val wordReduceWindow = words.reduceByWindow(_ +"*"+_, Seconds(3), Seconds(1))

    wordReduceWindow.print()
    ssc.start()

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}

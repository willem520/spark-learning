package willem.weiyu.bigData.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 获取窗口中的元素
  * @Date 2018/11/05 18:20
  */
object StreamingWindow {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("streamingDemo")
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)

    val lineWindow = lines.window(Seconds(3),Seconds(1))

    lineWindow.print()
    ssc.start()

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}

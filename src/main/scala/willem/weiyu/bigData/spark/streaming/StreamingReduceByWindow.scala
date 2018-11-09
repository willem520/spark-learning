package willem.weiyu.bigData.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 将窗口中元素进行reduce
  * @Date 2018/11/05 18:20
  */
object StreamingReduceByWindow {
  val MASTER = "local[4]"
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val BATCH_DURATION = 1
  //  val HOST = "localhost"
  val HOST = "10.26.27.81"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")

    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val ssc = new StreamingContext(conf,Seconds(BATCH_DURATION))
    ssc.checkpoint(CHECKPOINT_PATH)
    val lines = ssc.socketTextStream(HOST, 9999)

    /**
      * 根据正则分隔
      */
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

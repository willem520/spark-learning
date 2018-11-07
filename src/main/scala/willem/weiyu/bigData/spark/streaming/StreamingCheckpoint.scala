package willem.weiyu.bigData.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  * @description 有状态单词统计，可实现累加
  * @Date 2018/11/05 18:20
  */
object StreamingCheckpoint {
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val DURATION = 5
//  val HOST = "localhost"
  val HOST = "10.26.27.81"

  def main(args: Array[String]): Unit = {
    start();
  }

  def start(): Unit ={
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("streamingDemo")
    val ssc = StreamingContext.getOrCreate(CHECKPOINT_PATH,() => getStreamContext(conf,DURATION,CHECKPOINT_PATH))
    ssc.start()
    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }

  //从checkpoint恢复job上下文或者新建job上下文
  def getStreamContext(conf: SparkConf, duration : Int, checkpointDir : String) = {
    val ssc =  new StreamingContext(conf, Seconds(duration))
    ssc.checkpoint(checkpointDir)
    val lines = ssc.socketTextStream(HOST, 9999)

    /**
      * 按空格分隔
      */
    val words = lines.flatMap(_.split("\\s"))

    /**
      * 单个word变成tuple,并累加
      */
    val func = (oldVal:Seq[Int],newVal:Option[Int])=>{
      Some(oldVal.sum + newVal.getOrElse(0))
    }

    val wordCount = words.map(word =>(word, 1)).updateStateByKey[Int](func)

    wordCount.print()

    ssc
  }
}

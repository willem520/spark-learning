package willem.weiyu.bigData.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author weiyu
  */
object StreamingCheckpointDemo {
  val CHECKPOINT_PATH = "/spark/checkpoint"
  val DURATION = 5
  def main(args: Array[String]): Unit = {
    start();
  }

  def start(): Unit ={
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val conf = new SparkConf().setMaster("local[4]").setAppName("streamingDemo")
    val ssc = StreamingContext.getOrCreate(CHECKPOINT_PATH,() => {
      streamCreatFun(conf,DURATION,CHECKPOINT_PATH)
    })
    ssc.start()
    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }

  //从checkpoint恢复job上下文或者新建job上下文
  def streamCreatFun(conf: SparkConf, duration : Int, checkpointDir : String) = {
    val ssc =  new StreamingContext(conf, Seconds(duration))
    ssc.checkpoint(checkpointDir)
    val lines = ssc.socketTextStream("localhost", 9999)

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

    ssc
  }
}

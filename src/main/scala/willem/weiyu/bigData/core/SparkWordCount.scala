package willem.weiyu.bigData.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author weiyu@gomeholdings.com
  */
object SparkWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.err.println("Usage: SparkWordCount <inputfile>")
      System.exit(1)
    }

    //解决启动时hadoop报错问题
    //System.setProperty("hadoop.home.dir", "D:"+File.separator+"hadoop")

    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCountDemo")
    val sc = new SparkContext(conf)
    //wordcount操作，计算文件中包含Spark的行数
    val count = sc.textFile(args(0)).filter(line =>line.contains("Spark")).count()
    //打印结果
    println("count="+count)
    sc.stop()
  }
}

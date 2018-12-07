package willem.weiyu.bigData.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author weiyu
  * @Description map方法示例
  */
object MapTest {
  val MASTER = "local[*]"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val originInput = sc.parallelize(List("Samza","Storm","Flink","Spark"),2)
    val resultOutput = originInput.map(_ + " realtime")
    resultOutput.foreach(println)
  }
}

package willem.weiyu.bigData.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author weiyu
  */
object MapTest {
  val MASTER = "local[*]"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("bit","linc","xwc","fjg","wz","spark"),3)
    val b = a.map(_.length)
    val c = a.zip(b).collect()
    c.foreach(println)
  }
}

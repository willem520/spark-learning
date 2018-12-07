package willem.weiyu.bigData.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author weiyu
  * @Description flatMap方法示例
  */
object FlatMapTest {
  val MASTER = "local[*]"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 10, 5)
    val ret = a.flatMap(1 to _).collect()
    ret.foreach(println)
  }
}

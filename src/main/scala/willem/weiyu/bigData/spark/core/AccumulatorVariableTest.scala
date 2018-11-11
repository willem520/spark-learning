package willem.weiyu.bigData.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author weiyu
  * @Description 累加变量
  * 1.主要用于多个节点对一个变量进行共享性的操作
  * 2.Task只能对Accumulator进行累加操作，不能读取值
  * 3.Driver程序可以读取Accumulator的值
  * @Date 2018/11/11 16:30
  */
object AccumulatorVariableTest {
  val MASTER = "local[4]"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(MASTER).setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val sum = sc.longAccumulator

    val num = sc.parallelize(1 to 5, 1)
    num.foreach(sum.add(_))

    println(sum.value)
  }
}

package willem.weiyu.bigData.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author weiyu
  * @Description
  * 1.广播变量只会被发送到各个节点一次
  * 2.广播变量只能在Driver端定义，不能在Executor端定义
  * 3.广播变量可以在Driver端修改，不能在Executor端修改
  * @Date 2018/10/13 17:16
  */
object BroadcastVariableTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("broadcastVariableTest")
    val sc = new SparkContext(sparkConf)

    val factor = 3
    val broadcastFactor = sc.broadcast(factor)


    val num = sc.parallelize(1 to 10,3)
    val result = num.map(_ * broadcastFactor.value)
    result.foreach(println)
  }
}

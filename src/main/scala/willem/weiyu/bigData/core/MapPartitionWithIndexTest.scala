package willem.weiyu.bigData.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author weiyu
  */
object MapPartitionWithIndexTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitionWithIndexTest").setMaster("local")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10),3)

    def myFunc(index:Int,iter:Iterator[Int]):Iterator[String] = {
      iter.toList.map(x=>index+","+x).iterator
    }

    val ret = a.mapPartitionsWithIndex(myFunc).collect()
    ret.foreach(println)
  }
}

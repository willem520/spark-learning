package willem.weiyu.bigData.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author weiyu
  */
object MapPartitionsTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitionsTest").setMaster("local")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 9,3)
    def myFunc[T](iter:Iterator[T]):Iterator[(T,T)]={
      var res = List[(T, T)]()
      while (iter.hasNext){
        val cur = iter.next
        res.::=(cur,cur)
      }
      res.iterator
    }
    val ret = a.mapPartitions(myFunc)
    ret.foreach(println)
  }
}

package willem.weiyu.bigData.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingBlacklistDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("blacklistDemo")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val sc = ssc.sparkContext
    sc.setLogLevel("WARN")
    val blacklistRdd = sc.parallelize(Array(("Jim", true), ("hack", true)), 3)
    val listRdd = ssc.socketTextStream("localhost", 9999)
    val userRdd = listRdd.map(l =>(l.split(" ")(1), l))

    val validRddDS = userRdd.transform(ld => {
      //形如(k,(v,Some(w)))或(k,(v, None))
      val ljoinRdd = ld.leftOuterJoin(blacklistRdd)
      val fRdd = ljoinRdd.filter(tuple=>{
        if (tuple._2._2.getOrElse(false)){
          false
        }else{
          true
        }
      })
      val validRdd = fRdd.map(_._2._1)
      validRdd
    })
    validRddDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

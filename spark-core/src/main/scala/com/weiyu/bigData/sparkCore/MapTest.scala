package com.weiyu.bigData.sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author weiyu@gomeholdings.com
  */
object MapTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapTest").setMaster("local")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("bit","linc","xwc","fjg","wz","spark"),3)
    val b = a.map(_.length)
    val c = a.zip(b).collect()
    c.foreach(print)
  }
}

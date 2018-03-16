package com.weiyu.bigData.sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object ForeachPartitionTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("foreachPartitionTest").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.parallelize(List(1,2,3,4,5,6,7,8,9),3)
    input.foreachPartition(x => println((a,b) => x.reduce(a + b)))
  }
}

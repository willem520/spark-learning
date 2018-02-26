package com.gome.bigData.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
/**
  * @author weiyu@gomeholdings.com
  */
object SqlDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sqlDemo")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val reader = session.sqlContext.read
    val df = reader.json("E:\\people.json")
    df.printSchema
    println("======")
    df.show

    val schema = StructType(StructField("a", IntegerType)::StructField("b", StringType)::Nil)
  }
}

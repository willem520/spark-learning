package com.weiyu.bigData.simulator

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object JdbcDemo {
  val CORE_FLOW = "jie_core_flow"
  val TENDENCY_CHART = "jie_tendency_chart"
  val URL = "jdbc:mysql://10.143.90.34:3306/canal_test?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sqlDemo")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val reader = sparkSession.sqlContext.read
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "qa@test")
    val dataFrame = reader.jdbc(URL, CORE_FLOW, props)
    /*val schema = StructType(
      StructField("id",IntegerType)
        :: StructField("appno_num",IntegerType)
        :: StructField("appno_num_qoq",IntegerType)
        :: StructField("time48",StringType)
        :: StructField("insert_date", StringType)
        :: StructField("insert_time",DateType)
        ::Nil)*/
    import org.apache.spark.sql.functions._

    dataFrame.select("id","appno_num","appno_num_qoq","time48","insert_date","insert_time").where("insert_date='2018-01-30' AND start_code='1'").orderBy(desc("insert_time")).show(20)

  }
}

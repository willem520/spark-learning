package com.weiyu.bigData.simulator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StructuredStreamingKafkaDemo {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val spark = SparkSession.builder().master("local").appName("StructuredStreamingKafkaDmeo").getOrCreate()
    val inputStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "10.143.90.38:19092,10.143.90.39:19092,10.143.90.49:19092")
      .option("subscribe", "realtime_meijie_test")
      .option("startingOffsets", "earliest")
      .option("minPartitions", "10")
      .option("failOnDataLoss", "true")
      .load()

    val df = inputStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic").as("String").createTempView("test")
    val dFrame = spark.sql("select topic,value from test")

    //内置处理json
    /*val schema = StructType(StructField("SCHEMATABLE", StringType)::StructField("TABLE", StringType)::Nil)
    inputStream.select(col("key").cast("string"), from_json(col("value").cast("string"),schema))*/

    //import scala.util.parsing.json._
    /*val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()*/
    val query = dFrame.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()
  }
}


package com.weiyu.bigData.simulator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("StructuredStreamingDemo").getOrCreate()
    import sparkSession.implicits._

    val lines = sparkSession.readStream.format("socket").option("host","10.143.103.26").option("port",9999).load()

    val wordCount = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()
    wordCount.createOrReplaceTempView("test")
    val result = sparkSession.sql("select * from test")
    val query = result.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

    //new StructType().add("name","string").add("age","integer")
  }
}

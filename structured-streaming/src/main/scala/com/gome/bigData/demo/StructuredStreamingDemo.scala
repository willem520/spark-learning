package com.gome.bigData.demo

import org.apache.spark.sql.SparkSession

object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("StructuredStreamingDemo").getOrCreate()
    import sparkSession.implicits._

    val lines = sparkSession.readStream.format("socket").option("host","10.143.103.26").option("port",9999).load()

    /*val table = sparkSession.read.format("jdbc").options(
      Map("url"->"jdbc:mysql://10.143.90.34:3306/canal_test?useUnicode=true&characterEncoding=utf8&user=root&password=qa@test",
        "dbtable"->"jie_user")).load()*/
    val wordCount = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()

    wordCount.createOrReplaceTempView("test")
    val result = sparkSession.sql("select * from test")
    val query = result.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

    //new StructType().add("name","string").add("age","integer")
  }
}

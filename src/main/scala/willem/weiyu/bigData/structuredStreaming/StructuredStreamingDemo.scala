package willem.weiyu.bigData.structuredStreaming

import org.apache.spark.sql.SparkSession

object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val sparkSession = SparkSession.builder().master("local").appName("StructuredStreamingDemo").getOrCreate()
    import sparkSession.implicits._

    val lines = sparkSession.readStream.format("socket").option("host","localhost").option("port",9999).load()

    val wordCount = lines.as[String].flatMap(_.split(",|，|\\s+")).groupBy("value").count()
    wordCount.createOrReplaceTempView("word_count")
    val result = sparkSession.sql("select * from word_count")
    val query = result.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

  }
}

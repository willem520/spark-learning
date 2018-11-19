package willem.weiyu.bigData.spark.structuredStreaming

import org.apache.spark.sql.SparkSession

/**
  * @author weiyu
  * @description 单词统计
  * @Date 2018/01/05 18:20
  */
object StructuredStreaming {
  val MASTER = "local[4]"
//    val HOST = "localhost"
  val HOST = "10.26.27.81"
  val PORT = 9999;

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.5")
    val sparkSession = SparkSession.builder()
      .master(MASTER)
      .appName(getClass.getSimpleName).getOrCreate()

    val lines = sparkSession.readStream.format("socket")
      .option("host",HOST).option("port",PORT)
      .load()

    import sparkSession.implicits._
    val wordCount = lines.as[String].flatMap(_.split(",|，|\\s+")).groupBy("value").count()
    wordCount.createOrReplaceTempView("word_count")
    val result = sparkSession.sql("select * from word_count")
    val query = result.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

  }
}

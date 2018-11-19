package willem.weiyu.bigData.spark.structuredStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author weiyu
  * @description 单词统计
  * @Date 2018/01/05 18:20
  */
object StructuredStreamingKafka {
  val MASTER = "local[4]"
  val BOOTSTRAP_SERVER = "10.26.27.81:9092"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val spark = SparkSession.builder()
      .master(MASTER).appName(getClass.getSimpleName)
      .getOrCreate()
    val inputStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
      .option("subscribe", "test")
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


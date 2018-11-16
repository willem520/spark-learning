package willem.weiyu.bigData.spark.sql

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * @author weiyu
  */
object SparkJson {
  val MASTER = "local[4]"



  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master(MASTER)
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val schema = StructType(
      StructField("id", IntegerType)
        ::StructField("cityId", IntegerType)
        ::StructField("cityName", StringType)
        ::StructField("address", StringType)
        ::Nil)

    val df = sparkSession.read.schema(schema).json("example"+File.separator+"example.json")

    df.printSchema

    df.show

    sparkSession.close
  }
}

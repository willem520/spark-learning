package willem.weiyu.bigData.sql

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JdbcDemo {
  val CORE_FLOW = "jie_core_flow"
  val TENDENCY_CHART = "jie_tendency_chart"
  val URL = "jdbc:mysql://10.152.18.44:3306/canal_test?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull"
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

    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dataFrame.select("id","appno_num","appno_num_qoq","time48","insert_date","insert_time").where("insert_date='"+dateFormat.format(now)+"' AND start_code='1'").show(20)

  }
}

package willem.weiyu.bigData.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkJdbc {
  val MASTER = "local[4]"
  val MYSQL_URL = "jdbc:mysql://10.33.108.22:3307/rent_plat?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master(MASTER)
      .appName(getClass.getSimpleName)
      .getOrCreate()
    val props = new Properties()
    props.setProperty("user", "test")
    props.setProperty("password", "123456")
    val dataFrame = sparkSession.read.jdbc(MYSQL_URL, "rent_apartment_shop", props)

    dataFrame.show(20)

    sparkSession.close
  }
}

package willem.weiyu.bigData.sql

import org.apache.spark.sql.SparkSession

object HiveDemo {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("hiveDemo").config("spark.sql.warehouse.dir","spark-warehouse").enableHiveSupport().getOrCreate()
    val dbs = session.sql("show databases")
    dbs.show
  }
}

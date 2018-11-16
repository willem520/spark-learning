package willem.weiyu.bigData.spark.sql

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

/**
  * @Author weiyu
  * @Description
  * @Date 2018/11/16 12:18
  */
object SparkMongo {
  val MASTER = "local[4]"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master(MASTER)
      .appName(getClass.getSimpleName)
      .config("spark.mongodb.input.uri","mongodb://10.26.15.199:27017/rent_saas.0_rent_saas_business_resblock")
      .config("spark.mongodb.output.uri","mongodb://10.26.15.199:27017/rent_saas.0_rent_saas_business_resblock")
      .getOrCreate()

    val collection = MongoSpark.load(sparkSession)
    collection.show(10)

    sparkSession.close
  }
}

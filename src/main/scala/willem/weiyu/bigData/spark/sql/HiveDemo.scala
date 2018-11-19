package willem.weiyu.bigData.spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author weiyu
  */
object HiveDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("user.name", "gmjk");
    val sqlSession = SparkSession.builder().master("local").appName("hiveDemo").config("spark.sql.warehouse.dir","spark-warehouse").enableHiveSupport().getOrCreate()
    sqlSession.sql(
      """
        set hive.optimize.index.filter = false;
        set mapreduce.map.memory.mb=4096;
        set mapreduce.reduce.memory.mb=8192;
      """.stripMargin);
    sqlSession.sql("use redline")
    sqlSession.sql("SELECT app_no,id_card,send_time,dt,call_name,get_json_object(gate_way, '$.51_GJJ.response.data.result.basicInfo') as basic_info FROM inn_redline_calculate WHERE dt='20180805' AND call_name='51_GJJ'").createOrReplaceTempView("t_temp_basicInfo")
    val result = sqlSession.sql(
      """SELECT
        |app_no,
        |send_time,
        |dt,
        |call_name,
        |get_json_object(basic_info, '$.idNum') AS id_no,
        |get_json_object(basic_info, '$.name')  AS username,
        |get_json_object(basic_info, '$.cityPaid')  AS city,
        |get_json_object(basic_info, '$.comName')  AS company,
        |get_json_object(basic_info, '$.currentBalance')  AS balance
        |FROM t_temp_basicInfo limit 100""".stripMargin)
    //result.show

    result.write.mode(SaveMode.Overwrite).partitionBy("dt","call_name").saveAsTable("weiyu.t_basic_info")
  }
}

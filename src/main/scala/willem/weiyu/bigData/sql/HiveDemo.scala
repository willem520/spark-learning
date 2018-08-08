package willem.weiyu.bigData.sql

import org.apache.spark.sql.SparkSession

object HiveDemo {

  def main(args: Array[String]): Unit = {
    val sqlSession = SparkSession.builder().master("local").appName("hiveDemo").config("spark.sql.warehouse.dir","spark-warehouse").enableHiveSupport().getOrCreate()
    sqlSession.sql(
      """
        set hive.optimize.index.filter = false;
        set mapreduce.map.memory.mb=4096;
        set mapreduce.reduce.memory.mb=8192;
      """.stripMargin);
    sqlSession.sql("use redline")
    sqlSession.sql("SELECT app_no,id_card, get_json_object(gate_way, '$.51_GJJ.response.data.result.basicInfo') as basic_info FROM inn_redline_calculate WHERE dt='20180805' AND call_name='51_GJJ'").createOrReplaceTempView("t_temp_basicInfo")
    val result = sqlSession.sql(
      """SELECT
        |get_json_object(basic_info, '$.idNum') AS id_no,
        |get_json_object(basic_info, '$.name')  AS username,
        |get_json_object(basic_info, '$.cityPaid')  AS city,
        |get_json_object(basic_info, '$.comName')  AS company
        |FROM t_temp_basicInfo limit 100""".stripMargin)
      result.show
  }
}

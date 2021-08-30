package controller.collect

import org.apache.spark.sql.{DataFrame, Encoder}
import utils.Sparkutils

import java.util.Properties

object ExportData {
// private  val props = new  Properties()
// props.setProperty("user","shortloan")
// props.setProperty("password","root#Huirong104")
// props.setProperty("driver","com.mysql.jdbc.Driver")
// props.setProperty("url","jdbc:mysql://192.168.0.104:3306/car_dealers_test")

 /**
  * 根据表名和字段名生成对应dataframe
  * @param   tableName-输入表名参数
  * @param   colunms-Array格式输入字段名
  * @return  dataFrame格式输出结果
  */
 def getTableAsDF(tableName:String,colunms:Array[String]):DataFrame={

   val spark =  Sparkutils.initSparkSession("getTableData")
//spark.read.jdbc("","","",1,23,2,new Properties())
  //判断查询字段是全字段还是部分字段
    val cols = if (colunms.length == 0 || (colunms.length == 1 && colunms(0) == "*")) {
   "*"
  }else {
   colunms.mkString(",")
  }

  //  获取查询sql
  val sql =  s"select  $cols from  $tableName"

  /*
  *读取数据库数据表，以dataframe形式返回
  */
   val frame = spark.read.format("jdbc")
     .option("user", "shortloan")
     .option("password", "root#Huirong104")
     .option("driver", "com.mysql.jdbc.Driver")
     .option("numPartition", "10")
     .option("url", "jdbc:mysql://192.168.0.104:3306/car_dealers_test")
     .option("header", value = true)
     .option("query", sql)
     .load()
//   println(tableName+"的当前分区数为：" + frame.rdd.partitions.length)
   frame
 }

  //测试
 def main(args: Array[String]): Unit = {
  val cols_merc_loan =Array(
   "order_no",
   "loan_success_time"
  )
  val DF_mercLoan = ExportData.getTableAsDF("merc_order_loan", cols_merc_loan)
    .where("loan_success_time is not  null")
    .selectExpr("order_no", "UNIX_TIMESTAMP(loan_success_time) as fk_dt")
  DF_mercLoan.printSchema()
  DF_mercLoan.show(50,truncate = false)
   implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
   DF_mercLoan.map(data=> data.getValuesMap[Any](List("order_no","fk_dt")))
//     .show(20,false )
     .take(5)
     .foreach(println)
 }
}

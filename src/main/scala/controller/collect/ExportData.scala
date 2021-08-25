package controller.collect

import org.apache.spark.sql.DataFrame
import utils.Sparkutils

import java.util.Properties

object ExportData {
// private  val props = new  Properties()
// props.setProperty("user","shortloan")
// props.setProperty("password","root#Huirong104")
// props.setProperty("driver","com.mysql.jdbc.Driver")
// props.setProperty("url","jdbc:mysql://192.168.0.104:3306/car_dealers_test")

 def getTableAsDF(tableName:String,colunms:Array[String]):DataFrame={

  val spark =  Sparkutils.initSparkSession("getTableData")

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
  spark.read.format("jdbc")
    .option("user","shortloan")
    .option("password","root#Huirong104")
    .option("driver","com.mysql.jdbc.Driver")
    .option("url","jdbc:mysql://192.168.0.104:3306/car_dealers_test")
    .option("header",value = true)
//    .option("dbtable",tableName)
    .option("query",sql)
    .load()
//    jdbc(
//   props.getProperty("url"),
//   tableName,
//   props
//     )

 }
}

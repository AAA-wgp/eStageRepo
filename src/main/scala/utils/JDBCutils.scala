package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

object JDBCutils {
  private  val  props = new Properties()
  Class.forName("com.mysql.jdbc.Driver").newInstance()
  props.setProperty("user","shortloan")
   props.setProperty("password","root#Huirong104")
   props.setProperty("driver","com.mysql.jdbc.Driver")
   props.setProperty("url","jdbc:mysql://192.168.0.104:3306/car_dealers_test")
  props.setProperty("characterEncoding","utf-8")
  def getDatabaseConn:  Connection ={
     DriverManager.getConnection("jdbc:mysql://192.168.0.104:3306/car_dealers_test", props)

}
  def closeConnection(conn:Connection,pstmt:PreparedStatement): Unit ={
    try {
      if (pstmt != null){
        pstmt.close()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      conn.close()
    }
  }


}
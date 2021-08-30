//package applications
//
//import bean.Merc_loan
//import org.apache.spark.rdd.JdbcRDD
//import utils.JDBCutils
//import utils.Sparkutils.initSparkConf
//
//object TestCode {
//  def main(args: Array[String]): Unit = {
//    val  sc = initSparkConf("test")
//    val  loanRDD  = new JdbcRDD[Merc_loan](
//      sc,
//      JDBCutils.getDatabaseConn,
//      "select order_no,loan_success_time from  merc_order_loan where ?<=id and  id<=?",
//      1,
//      473311,
//      20,
//      rs=>{Merc_loan(rs.getString(1),rs.getString(2))}
//    )
//    loanRDD.foreach(println)
//
//
//
//    sc.stop()
//  }
//
//}

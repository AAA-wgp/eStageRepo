package applications

import bean.{Business_data, Merc_loan, Merc_order, Tb_account}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.storage.StorageLevel
import utils.{JDBCutils, Sparkutils}


object merc_order_withRDD_Demo {
  def main(args: Array[String]): Unit = {
    val sc = Sparkutils.initSparkConf("getStageRatio")


      //获取订单主表数据
      val orderRDD: JdbcRDD[Merc_order] = new JdbcRDD[Merc_order](
        sc,
        () => JDBCutils.getDatabaseConn,
        "select order_no,shop_id,platform_code,order_state,loan_amount  from merc_order where ?<= id and id<=?",
        32,
        4726002,
        20,
        rs => {
          Merc_order(rs.getString(1),
            rs.getString(2),
            rs.getString(3),
            rs.getInt(4),
            rs.getDouble(5))
        }
      )
    //主订单表数据缓存
    orderRDD.persist(StorageLevel.MEMORY_AND_DISK)
//    orderRDD.take(20).foreach(println)

    //获取业务库数据
      val businessRDD = new JdbcRDD[Business_data](
        sc,
        () => JDBCutils.getDatabaseConn,
        "select order_no,store_id,loan_or_not,by_stages,substr((lending_time),1,10) as lending_time,group_id from  business_data where ?<= t_id and t_id<=?",
        46,
        1314,
        20,
        rs => {
          Business_data(rs.getString(1),
            rs.getString(2),
            rs.getString(3),
            rs.getString(4),
            rs.getString(5),
            rs.getString(6))
        }
      )

      //获取 订单分期信息（放款时间）
      val loanRDD = new JdbcRDD[Merc_loan](
        sc,
        () => JDBCutils.getDatabaseConn,
        "select order_no,substr((loan_success_time),1,10)  as  loan_success_time from  merc_order_loan where ?<=id and  id<=? and loan_success_time is not null",
        1,
        473311,
        20,
        rs => {
          Merc_loan(rs.getString(1), rs.getString(2))
        }
      )

      //数据预处理,转换为pairRDD
      val loanPreRDD = loanRDD.map(
        data => (data.order_no, data.loan_success_time)
      )

      //数据预处理,转换为pairRDD
      val businessPreRDD = businessRDD.map(
        data => {
          val shop_id = data.store_id
          val platform_code = data.group_id
          val loan_success_time = data.lending_time
          (data.order_no, (shop_id, platform_code, data.loan_or_not, data.by_stages, loan_success_time))
        }
      )
      //数据逻辑处理
    orderRDD.filter(_.order_state > 3)
      .map(
        data => {
          val loan_or_not = data.order_state match {
            case 10 => "是"
            case _ => "否"
          }
          val by_stages = data.loan_amount match {
            case 0 => "全款"
            case _ => "分期"
          }

          (data.order_no, (data.shop_id, data.platform_code, loan_or_not, by_stages))
        }
      )
      .leftOuterJoin(loanPreRDD)
      .map(
        data => {
          val loan_success_time: String = data._2._2 match {
            case Some(value) => value
            case _ => ""
          }
          (data._1,
            (data._2._1._1,
              data._2._1._2,
              data._2._1._3,
              data._2._1._4,
              loan_success_time))
        }
      )
      .union(businessPreRDD)
      .map(
        data => {
          val  str =  if( data._2._5 == null ) {
            ""
          }else  if( data._2._5.length >7 ){
             data._2._5.substring(0,7)
          }
          val stageAndLoan = if (data._2._3 == "是" && data._2._4 == "分期") 1 else 0
          ((data._2._2, data._2._1,str ),
            (1, stageAndLoan))
        }
      )
      .reduceByKey(
        (data1,data2)=>(data1._1+data1._1,data1._2+data2._2)
      )
      .map(
        data => (data._1, (data._2._2*1000.toDouble / data._2._1/10).toString.concat("%"))
      )
      .collect()
      .foreach(println)

    //关闭SparkContext
    sc.stop()
    }





}

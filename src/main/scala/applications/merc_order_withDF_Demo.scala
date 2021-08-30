package applications

import bean.{Merc_ResultInfo, Buff}
import controller.collect.{ExportData, GetMercShopDetailInfo}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import utils.Sparkutils

object merc_order_withDF_Demo {
  def main(args: Array[String]): Unit = {
    val spark = Sparkutils.initSparkSession("getStageLoanRatio")
    import  spark.implicits._
     // 定义订单表查询字段数组
    val cols=Array(
      "order_no",
      "shop_id",
      "platform_code",
      "seller_acc_id",
      "order_state",
      "customer_name",
      "customer_phone",
      "loan_amount",
      "period_nums",
      "create_time",
      "customer_cert_no",
      "UNIX_TIMESTAMP(submit_time)"
    )

    //定义业务库查询字段数组
    val cols_business =Array(
      "order_no",
      "store_id",
      "loan_or_not",
      "by_stages",
//      "business_bank",
//      "reporting_time",
//      "is_it_approved",
      "group_id",
      "UNIX_TIMESTAMP(lending_time)",//放款时间
//      "installment_amount",//分期金额
//      "number_of_stages",

      "name_of_salesman"
    )

    //定义订单分期贷款信息表字段数组
    val cols_merc_loan =Array(
      "order_no",
      "UNIX_TIMESTAMP(loan_success_time)"
    )

    //定义账户信息表字段数组
    val cols_tb_account = Array("acc_id",
    "name",
    "state",
    "del_flag"
    )

    //    以DF形式获取订单的数据
    val frame_order: DataFrame = ExportData.getTableAsDF("merc_order",cols)
      .withColumnRenamed("submit_time","UNIX_TIMESTAMP(submit_time)")

    //    以DF形式获取业务库订单的数据
    val frame_business = ExportData.getTableAsDF("business_data", cols_business)
      .withColumnRenamed("lending_time","loan_success_time")
      .withColumnRenamed("group_id","platform_code")
      .withColumnRenamed("store_id","shop_id")
      .withColumnRenamed("UNIX_TIMESTAMP(lending_time)","lending_time")

    //    以DF形式获取订单分期贷款信息表数据
    val frame_merc_loan = ExportData.getTableAsDF("merc_order_loan", cols_merc_loan)
      .withColumnRenamed("UNIX_TIMESTAMP(loan_success_time)","loan_success_time")
    //   以DF形式获取账户信息表字段数组
    val frame_tb_role = ExportData.getTableAsDF("tb_account", cols_tb_account)
      .where("state =0 and del_flag=1")

    //   以DF形式获取门店展平后字段数组
    val frame_merc_detail_info: DataFrame = GetMercShopDetailInfo
      .getMercShopFlattenInfo
      .drop(("platform_code"))

    /*
    * 获取订单表对应字段，关联merc_order_loan获取放款时间
    * 关联tb_role表获取对应销售员姓名
    * 最后与业务库数据（business_data）进行合并
    * */
    val frame = frame_order.filter(col("order_state") > 3)
      //      .where("order_stage >3")
      .selectExpr("order_no",
        "shop_id",
        "seller_acc_id",
        //        "name as name_of_salesman",
        "case order_state  when 10 then  '是' else '否' end as loan_or_not ",
        "case loan_amount when 0 then '全款' else '分期' end  as by_stages",
//        """
//          |case order_state when 4 then '准入审批中'
//          |when 5 then '准入通过'
//          |when 6 then '准入拒绝'
//          |when 7 then '分期审批中'
//          |when 8 then '分期审批拒绝'
//          |when 9 then '待放款'
//          |when 10 then '已放款'
//          |when 11 then '放款拒绝'
//          |end  as  order_state
//          |""".stripMargin,
        "platform_code")
      .join(
        frame_merc_loan,
        frame_order("order_no") === frame_merc_loan("order_no"),
        "left")
      .drop(frame_merc_loan("order_no"))

      val frame_union_order_info =  frame
      .join(frame_tb_role, frame_order("seller_acc_id") === frame_tb_role("acc_id"), "left")
      .drop(frame_tb_role("acc_id"))
      .drop(frame_tb_role("state"))
      .drop(frame_tb_role("del_flag"))
      .drop(frame_order("seller_acc_id"))
      .withColumnRenamed("name", "name_of_salesman")
      .union(frame_business)
//    frame_union_order_info.show(100,false)
    /*
    * 将合并后数据关联门店展平后详细数据获取对应一级部门、二级部门等信息
    * */
    frame_union_order_info.createGlobalTempView("order_union_info")
    frame_merc_detail_info.createGlobalTempView("merc_shop_detailInfo")

//    数据持久化，后续需要分订单全款或者分期来计算指标
//    frame_with_Merc_detail_info.persist(StorageLevel.MEMORY_AND_DISK)
//    以放款时间为维度计算销量、分期且放款量等


//    frame_with_Merc_detail_info.selectExpr(
//      "order_no",
//      "shop_id",
//      "case loan_or_not when '是' then (case by_stages when '分期' then 1 end)  else null end as stage_and_loan",
//      "platform_code",
//      "loan_success_time",
//      "company_name",
//      "firstLevelID",
//      "firstLevelName",
//      "secondLevelID",
//      "secondLevelName"
//    )
//      .groupBy(
//      "platform_code",
//      "company_name",
//      "firstLevelID",
//      "firstLevelName",
//      "secondLevelID",
//      "secondLevelName"
//    ).agg("stage_and_loan"->"sum","order_no"->"sum")
//      .withColumnRenamed("sum(stage_and_loan)","stage_loan_order_nums")
//      .withColumnRenamed("sum(order_no)","nums")
//      .selectExpr(
//        "platform_code",
//        "company_name",
//        "firstLevelID",
//        "firstLevelName",
//        "secondLevelID",
//        "secondLevelName",
//        "stage_loan_order_nums/nums  AS STAGE_ORDER_RATIO"
//      )
//      .show(20,truncate = false)
 spark.sql(
  """
    |select oui.*,
    | msd.company_name
    |,msd.thirdLevelID
    |,msd.thirdLevelName
    |,msd.firstLevelID
    |,msd.firstLevelName
    |,msd.secondLevelID
    |,msd.secondLevelName
    |from  global_temp.order_union_info oui
    |left join global_temp.merc_shop_detailInfo msd
    |on oui.shop_id=msd.shop_id
    |""".stripMargin).as[Merc_ResultInfo]
   .createTempView("detailInfo")
    spark.udf.register("avgStageLoan", "GetStageRatioFunction")
    spark.sql("select avgStageLoan,shop_id,platform_code,company_name  from detailInfo group by shop_id,platform_code,company_name ").show(20,truncate = false)
  }
  class GetStageRatioFunction  extends Aggregator[Merc_ResultInfo,Buff,Double]{
    override def zero: Buff = Buff(0,0)

    override def reduce(b: Buff, a: Merc_ResultInfo): Buff = {
      b.sum += 1
      if(a.by_stages=="分期" && a.loan_or_not=="是"){
        b.stageAndLoan += 1
      }
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.stageAndLoan += b2.stageAndLoan
      b1
    }

    override def finish(reduction: Buff): Double = reduction.stageAndLoan.toDouble/reduction.sum

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}

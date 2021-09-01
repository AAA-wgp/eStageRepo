package applications

import bean.{Buff, Merc_ResultInfo}
import controller.collect.{ExportData, GetMercShopDetailInfo}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, functions}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
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
      "submit_time"
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
      "name_of_salesman",
      "lending_time"//放款时间
    )

    //定义订单分期贷款信息表字段数组
    val cols_merc_loan =Array(
      "order_no",
      "loan_success_time"
    )

    //定义账户信息表字段数组
    val cols_tb_account = Array("acc_id",
    "name",
    "state",
    "del_flag"
    )

    //    以DF形式获取订单的数据
    val frame_order: DataFrame = ExportData.getTableAsDF("merc_order",cols)

    //    以DF形式获取业务库订单的数据
    val frame_business = ExportData.getTableAsDF("business_data", cols_business)
      .withColumnRenamed("group_id","platform_code")
      .withColumnRenamed("store_id","shop_id")
      .withColumnRenamed("lending_time","loan_success_time")
      .withColumn("loan_success_time",substring(col("loan_success_time"),1,10))
    //    以DF形式获取订单分期贷款信息表数据
    val frame_merc_loan = ExportData.getTableAsDF("merc_order_loan", cols_merc_loan)
      .withColumn("loan_success_time",substring(col("loan_success_time"),1,10))

    //   以DF形式获取账户信息表字段数组
    val frame_tb_role = ExportData.getTableAsDF("tb_account", cols_tb_account)
      .where("state =0 and del_flag=1")

    //   以DF形式获取门店展平后字段数组
    val frame_merc_detail_info: DataFrame = GetMercShopDetailInfo
      .getMercShopFlattenInfo
      .drop("platform_code")

    /*
    * 获取订单表对应字段，关联merc_order_loan获取放款时间
    * 关联tb_role表获取对应销售员姓名
    * 最后与业务库数据（business_data）进行合并
    * */
    val frame_union_order_info = frame_order.filter(col("order_state") > 3)
      //      .where("order_stage >3")
      .selectExpr("order_no",
        "shop_id",
        "seller_acc_id",
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

      .join(frame_tb_role, frame_order("seller_acc_id") === frame_tb_role("acc_id"), "left")
      .drop(frame_tb_role("acc_id"))
      .drop(frame_tb_role("state"))
      .drop(frame_tb_role("del_flag"))
      .drop(frame_order("seller_acc_id"))
      .withColumnRenamed("name", "name_of_salesman")
      .join(
        frame_merc_loan,
        frame_order("order_no") === frame_merc_loan("order_no"),
        "left")
      .drop(frame_merc_loan("order_no"))
      .union(frame_business)
    /*
    * 将合并后数据关联门店展平后详细数据获取对应一级部门、二级部门等信息
    * */
    frame_union_order_info.createGlobalTempView("order_union_info")
    frame_merc_detail_info.createGlobalTempView("merc_shop_detailInfo")
    val detailInfo: DataFrame = spark.sql(
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
        |""".stripMargin)


    //注册自定义函数
    spark.udf.register("avgStageLoan", functions.udaf( new GetStageRatioFunction))

//    detailInfo.printSchema()
    //计算得出分组后详细结果
    detailInfo.repartition(1)
      .groupBy("shop_id","loan_success_time")
      .agg("order_no"->"avgStageLoan")
      .show(20,truncate = false)

    //关闭sparkSession连接
    spark.stop()
  }

    //自定义udaf获取门店对应放款时间的分期放款销量占比
  class GetStageRatioFunction  extends Aggregator[Merc_ResultInfo,Buff,Double]{
    override def zero: Buff = Buff(0,0)

    //根据Merc_ResultInfo表by_stages与loan_or_not字段判断订单是否为分期且放款
    override def reduce(b: Buff, a: Merc_ResultInfo): Buff = {
      b.sum += 1
      if(a.by_stages == "分期" && a.loan_or_not == "是"){
        b.stageAndLoan += 1
      }
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.stageAndLoan += b2.stageAndLoan
      b1
    }

    override def finish(reduction: Buff): Double = reduction.stageAndLoan.toDouble/reduction.sum.toDouble

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}

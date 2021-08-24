package applications

import bean.Merc_order
import controller.collect.ExportData
import org.apache.spark.sql.DataFrame

object merc_order_Test {
  def main(args: Array[String]): Unit = {
//    val tupled = Merc_order.tupled
    val cols=Array("order_no",
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
    val frame: DataFrame = ExportData.getTableAsDF("merc_order",cols)
    frame.show(1)
  }

}

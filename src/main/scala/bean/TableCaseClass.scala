package bean

//创建订单表样例类
case class  Merc_order(
                        order_no:String, //订单号
                        shop_id:String, //店铺ID
                        platform_code:String,//集团ID
                        seller_acc_id:String,//销售员ID
                        order_state:Int,//订单状态
                        customer_name:String,//客户姓名
                        customer_phone:String,//客户手机号
                        loan_amount:Double,//贷款金额
                        period_nums:Int,//分期期数
                        create_time:Long,//创建时间
                        customer_cert_no:String,//客户身份证号
                        submit_time:Long//提交时间
                      )
//创建业务库业务数据
case   class Business_data(
                          group_name:String,//集团名称
                          store_name:String,//门店名称
                          customer_name:String,//客户名称
                          telephone:String,//客户手机号
                          id_number:String,//客户身份证号
                          name_of_salesman:String,//销售员姓名
                          by_stages:String,//全款分期
                          business_bank:String,//业务行名称
                          reporting_time:Long,//提报时间
                          is_it_approved:String,//是否通过审批
                          loan_or_not:String,//是否放款
                          lending_time:Long,//放款时间
                          installment_amount:Double,//分期金额
                          number_of_stages:Int,//分期期数
                          group_id:String,//集团ID
                          store_id:String//门店ID
                          )
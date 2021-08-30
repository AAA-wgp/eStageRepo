package controller.collect

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import utils.Sparkutils
object GetMercShopDetailInfo {
   private val  spark  = Sparkutils.initSparkSession("getMercShopDetailInfo")

  /**
   * 获取Merc_shop表根据层级关系，展平之后的结果，分别对应门店、二级部门、一级部门以上的详细信息
   * @return DataFrame
   */
  def getMercShopFlattenInfo:DataFrame = {
//    import  spark.implicits._
    val col_merc_shop =Array("shop_id",
    "platform_code",
    "shop_name",
    "level",
    "parent_id",
    "parent_level")
    val col_acc_company=Array("platform_code","company_name")
    val df_merc_shop: DataFrame = ExportData.getTableAsDF("merc_shop", col_merc_shop)
    val df_acc_company: DataFrame = ExportData.getTableAsDF("acc_company", col_acc_company)
    var df_join_tab = df_merc_shop.join(
      df_acc_company,
      df_merc_shop("platform_code")===df_acc_company("platform_code")
    ).drop(df_acc_company("platform_code"))
    df_join_tab.createTempView("merc_shop_info")
    val df_merc_shop_flatten: DataFrame = spark.sql(
      """
        |select  msi3.shop_id,msi3.platform_code,msi3.company_name,msi3.shop_id as thirdLevelID,msi3.shop_name as thirdLevelName ,merc_level2.firstLevelID,merc_level2.firstLevelName,merc_level2.secondLevelID,merc_level2.secondLevelName from   merc_shop_info msi3 left join(
        |select  msi2.shop_id,msi2.platform_code,msi2.company_name,msi2.shop_id as secondLevelID,msi2.shop_name as secondLevelName ,merc_level1.firstLevelID,merc_level1.firstLevelName from   merc_shop_info msi2 left join(
        |select shop_id,platform_code,company_name,shop_id as firstLevelID,shop_name as firstLevelName  from   merc_shop_info  where level =1) merc_level1
        |on   msi2.parent_id=merc_level1.shop_id and msi2.platform_code=merc_level1.platform_code where msi2.level=2) merc_level2 on msi3.parent_id=merc_level2.shop_id and msi3.platform_code=merc_level2.platform_code  where msi3.level=3
        |union all(
        |select  msi2.shop_id,msi2.platform_code,msi2.company_name,"" as thirdLevelID,"" as thirdLevelName,msi2.shop_id as secondLevelID,msi2.shop_name as secondLevelName ,merc_level1.firstLevelID,merc_level1.firstLevelName from   merc_shop_info msi2 left join(
        |select shop_id,platform_code,company_name,shop_id as firstLevelID,shop_name as firstLevelName  from   merc_shop_info  where level =1) merc_level1
        |on   msi2.parent_id=merc_level1.shop_id and msi2.platform_code=merc_level1.platform_code  where msi2.level=2)
        |union all(
        |select shop_id,platform_code,company_name,"" as thirdLevelID,"" as thirdLevelName,"" as secondLevelID,"" as secondLevelName , shop_id as firstLevelID,shop_name as firstLevelName  from   merc_shop_info  where level =1)
        |""".stripMargin)
    spark.sqlContext.dropTempTable("merc_shop_info")
    df_merc_shop_flatten
  }

  def main(args: Array[String]): Unit = {
    getMercShopFlattenInfo.printSchema()
    val spark  =  Sparkutils.initSparkSession("mercShopTest")
    import  spark.implicits._
    getMercShopFlattenInfo
      .select("shop_id")
//      .withColumn("num1",lit("1"))
//      //  .filter($"firstLevelName" === "一家亲")
//      .groupBy('firstLevelName)
//      .agg(count(col("num1")))
      .show(35,truncate = false)

//  .show()
//      .show(50,false)
  }
}

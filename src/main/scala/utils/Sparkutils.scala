package utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Sparkutils {
// val  spark =
//  初始化sparkContext
  def initSparkConf(appName:String,mode:String="local[*]"): SparkContext ={
    val conf = new SparkConf().setAppName(appName).setMaster(mode)
     new SparkContext(conf)

  }

  //初始化sparkSession
 def initSparkSession(appName:String,mode:String="local[*]"): SparkSession ={
   val conf = new SparkConf().setAppName(appName).setMaster(mode)
   SparkSession.builder().config(conf).getOrCreate()
 }
  def main(args: Array[String]): Unit = {
   val spark =  initSparkSession("test")
//    spark.read.jdbc()

  }
}

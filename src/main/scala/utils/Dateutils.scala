package utils

//import java.sql.Date
import org.apache.commons.lang.time.FastDateFormat

import java.text.SimpleDateFormat
import java.util.Date

object Dateutils {
//  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
//  private val date = new Date()
private val fdf: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")

  /**
   * 通过Long类型时间戳转换为 字符串日期格式
   * @param timestamp 输入Long类型格式
   * @return
   */
  def getDate(timestamp:Long): String ={
  fdf.format(timestamp)
}

  def main(args: Array[String]): Unit = {
    println(getDate(1625640620))
  }
}

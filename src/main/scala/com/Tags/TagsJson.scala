package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsJson extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[List[(String, Int)]]
    //获取广告类型，广告类型名称
    val aa = row.map(_._1)
      list:+=("MMMM"+aa,1)


    list
  }
}

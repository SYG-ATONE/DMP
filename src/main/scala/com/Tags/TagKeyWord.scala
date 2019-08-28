package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagKeyWord extends Tag {
  /**
    * 关键字
    */
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val stopword = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    val row = args(0).asInstanceOf[Row]
    row.getAs[String]("keywords")
      .split("\\|")
      .filter(word => {
        word.length >= 3 && word.length <= 8 && !stopword.value.contains(word)
      })
      .foreach(word => list :+= ("K" + word, 1))

    list
  }
}

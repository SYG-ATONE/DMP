package com.Tags

import com.utils.{JedisUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsApp extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //处理参数的数据类型
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    //获取appid 和 appname
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")

    //空值判断
    if (StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if (StringUtils.isNoneBlank(appid)){
     // list :+= ("APP"+appmap.value.getOrElse(appid,appid),1)
     val appname =  jedis.get("appid")
      list:+=("APP"+appname,1)
    }
    list
  }
}

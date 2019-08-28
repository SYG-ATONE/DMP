package com.Tags

/**
  * 逻辑问题：getBusiness方法
  * 先转换成geohash，然后通过geohash 去数据库中去查询（为了节省经费），
  * 如果查询不到则去高德地图中解析商圈，然后存入redis
  */

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtil, JedisUtils, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 商圈标签
  */
object BusinessTag extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    //获取经纬度，过滤经纬度
    if (Utils2Type.toDouble(long) >= 73 && Utils2Type.toDouble(long) <= 135
      && Utils2Type.toDouble(lat) >= 3.86 && Utils2Type.toDouble(lat) <= 54) {

      //先去数据库获取商圈
      val business = getBusiness(long.toDouble, lat.toDouble)

      //判断此商圈是否为空
      if (StringUtils.isNotBlank(business)) {
        val lines = business.split(",")
        lines.foreach(f =>list:+=(f,1))

      }
    }
    list
  }


  /**
    * 获取商圈信息
    */

  def getBusiness(long: Double, lat: Double): String = {
    //转换GeoHash字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)

    //先去数据库查询   节省经费
    var business = redis_queryBusiness(geohash)
    //判断商圈是否为空
    if (business == null || business.length == 0) {
      //通过经纬度获取商圈
      business = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)
      //如果调用高德解析商圈，那么需要将次商圈存入redis
      redis_inserBusiness(geohash,business)
    }
    business
  }

  /**
    * 查询商圈信息
    */
  def redis_queryBusiness(geohash: String): String = {
    val jedis = JedisUtils.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储到redis
    */
  def redis_inserBusiness(geoHash: String, business: String) = {
    val jedis = JedisUtils.getConnection()
    jedis.set(geoHash, business)
    jedis.close()
  }
}

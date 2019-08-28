package com.Tags


import com.utils.{JedisUtils, TagUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.collection.immutable

/**
  * 上下文标签
  */
object test {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录不匹配")
      sys.exit()
    }

    val Array(intputPath) = args
    //创建上下文
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.parquet(intputPath)
    df.rdd.map(row =>{


      val userId = TagUtils.getOneUserId(row)
      val business: List[(String, Int)] = BusinessTag.makeTags(row)
      (userId,business)
    })
      .reduceByKey((list1,list2) =>(
        list1:::list2
      )).groupBy(_._1)
      .foreach(println)
  }
}
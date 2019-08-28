package com.Tags

import com.utils.{JedisUtils, TagUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.collection.immutable

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("目录不匹配")
      sys.exit()
    }

    val Array(intputPath, outputPath, dirPath, keywordPath) = args
    //创建上下文
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec","snappy")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.parquet(intputPath)

    //读取字典文件
    val map = spark.sparkContext.textFile(dirPath)
      .map(_.split("\\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collect.toMap
    //val bd: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(map)

    val stopwords = spark.sparkContext.textFile(keywordPath)
      .map(x => (x, 1)).collectAsMap()
    val bdword: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopwords)

    //过滤符合id的数据,拿出用户
    val id = df.filter(TagUtils.OneUserId)

    //进行标签的实现
    val res = id.rdd.map(row => {
      val jedis: Jedis = JedisUtils.getConnection()
      //          map.map(a=>{
      //            jedis.set(a._1,a._2)
      //          })
      //取出用户id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过row数据，按照要求打上所有的标签
      //对广告位类型和广告广告类型名称打标签

      val adList = TagsAd.makeTags(row)

      val appList: List[(String, Int)] = TagsApp.makeTags(row, jedis)
      //App名称打标签

      //关键字打标签
      val wordList = TagKeyWord.makeTags(row, bdword)

      //渠道标签
      // val channel = row.getAs[Int]("")
      val dvList = TagDevice.makeTags(row)
      val locationList = Taglocation.makeTags(row)

      //商圈标签
      val business = BusinessTag.makeTags(row)


      jedis.close()
      (userId, adList ++ appList ++ wordList ++ dvList ++ locationList ++ business)
    })
      .reduceByKey((list1,list2) =>
    //对list集合进行拼接  List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2).groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    )
    res.collect().foreach(println)
    spark.close()
  }

}

package com.Tags

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object aaa {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("a")
    val sc = new SparkContext(conf)
    val data: Map[String, String] = sc.textFile("C://Users/shiyagung/Desktop/Spark用户画像分析/app_dict.txt")
      .map ( _.split ( "\\t" ) ).filter ( _.size >= 5 ).map ( a => {
      (a ( 4 ), a ( 1 ))
    } ).collect().toMap
    //println(data)

    val jedis: Jedis =new Jedis("hadoop02",6379)
    data.map(a=>{

      jedis.set(a._1,a._2)

    })
    //val str: String = jedis.get("app_dict")
data.map(x => x._1)
jedis.get("id")
    jedis.close()
    sc.stop()
  }
}

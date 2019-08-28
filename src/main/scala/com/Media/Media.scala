package com.Media

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.utils.MediaUtils

object Media {
  def main(args: Array[String]): Unit = {


    if (args.length!=1){
      println("输入目录参数错误，退出程序")
      sys.exit()
    }
    val Array(inputPath) = args
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    /**
      * 广播变量
      */
    val bd = spark.sparkContext.textFile("C://Users/shiyagung/Desktop/Spark用户画像分析//app_dict.txt")
      .map(_.split("\\t"))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))

    //将元组(appid,appname)
    val bbc: Broadcast[Array[(String, String)]] = spark.sparkContext.broadcast(bd.collect)

   val tups =  spark.read.parquet(inputPath).rdd.map(
      row => {
        //把需要的数据全部得到
        val requestmode = row.getAs[Int]("requestmode")
        val processnode = row.getAs[Int]("processnode")
        val iseffective = row.getAs[Int]("iseffective")
        val isbilling = row.getAs[Int]("isbilling")
        val isbid = row.getAs[Int]("isbid")
        val iswin = row.getAs[Int]("iswin")
        val adorderid = row.getAs[Int]("adorderid")
        val WinPrice = row.getAs[Double]("winprice")
        val adpayment = row.getAs[Double]("adpayment")
        // key 值  应用id
        val appid = row.getAs[String]("appid")
        val appname = row.getAs[String]("appname")
        //定义一个临时变量
        var app_name = ""


        val a =MediaUtils.AA(requestmode,processnode)
        val b = MediaUtils.BB(requestmode,iseffective)
        val c = MediaUtils.CC(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)

        if(appname.equals("未知") || appname.equals("其他")){
          app_name = bbc.value.toMap.getOrElse(appid,null)
        }else{
          app_name = appname
        }

        (app_name,a++b++c)
      }
    )

    val res: RDD[(String, List[Double])] = tups.filter(_._1 != null).reduceByKey((list0, list1) => {
      list0.zip(list1).map(x => (x._1 + x._2))
    })
    res
    res.collect().toBuffer.foreach(println)

    spark.stop()
  }
}

package com.Net

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.utils.TermiUtils

object Net {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if (args.length != 1) {
      println("参数目录不正确，退出程序")
      sys.exit()
    }
    //设置输入和输出路径（目录）

    val Array(inputPath) = args
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
      //设置序列化的方式，采用Kryo的序列化方式，比默认的额序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    //设置压缩方式，使用snappy进行压缩
    sqlcontext.setConf("spark.sql.parquet.compression.codec","snappy")
    val df = sqlcontext.read.parquet(inputPath)

    //将数据进行处理，统计各个指标
    val res =  df.rdd.map(row => {
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
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      val ispname = row.getAs[String]("ispname")

      val networkmannername = row.getAs[String]("networkmannername")
      // 创建三个对应的方法处理九个指标
      val a = TermiUtils.request(requestmode, processnode)
      val b = TermiUtils.click(requestmode, iseffective)
      val c = TermiUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      ((networkmannername), a ++ b ++ c)
    })
      .reduceByKey((a,b) => {
        a.zip(b).map(x => x._1+x._2)
      })
    res.collect().toBuffer.foreach(println)
  }
}

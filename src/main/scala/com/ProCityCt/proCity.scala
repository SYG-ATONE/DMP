package com.ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 需求指标二  统计各个省市分布情况
  */
object proCity {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if (args.length != 2) {
      println("参数目录不正确，退出程序")
      sys.exit()
    }
    //设置输入和输出路径（目录）

    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
      //设置序列化的方式，采用Kryo的序列化方式，比默认的额序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    //设置压缩方式，使用snappy进行压缩
    sqlcontext.setConf("spark.sql.parquet.compression.codec","snappy")
    val df = sqlcontext.read.parquet(inputPath)
    df.createTempView("log")
    val res = sqlcontext.sql("select count(*) ct,provincename,cityname from log group by provincename,cityname")

    //res.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)

    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("append")
      .jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)
    sc.stop()

  }
}
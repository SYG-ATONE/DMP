package com.ETL

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
  import com.utils.{SchemaUtils, Utils2Type}

  object txt2ParquetRdd {
    def main(args: Array[String]): Unit = {
      //判断路径是否正确
      if(args.length != 2){
        println("参数目录不正确，退出程序")
        sys.exit()
      }
      //设置输入和输出路径（目录）

      val Array(inputPath,outputPath) = args
      val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
        //设置序列化的方式，采用Kryo的序列化方式，比默认的额序列化方式性能高
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      //创建执行入口
      val sc = new SparkContext(conf)
      val lines: RDD[String] = sc.textFile(inputPath)
      val tups: RDD[(String, Int)] = lines.map(x => {
        val splited = x.split(",", -1)
        val provincename = splited(24)
        val cityname = splited(25)
        val tup = provincename + "_" + cityname
        (tup, 1)
      })
      val res: RDD[(String, Int)] = tups.reduceByKey(_+_)
      println(res.collect().toBuffer)

    }
  }


package com.ETL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import com.utils.{SchemaUtils, Utils2Type}

object txt2Parquet {
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
    val sqlcontext = new SQLContext(sc)

    //设置压缩方式，使用snappy进行压缩
    sqlcontext.setConf("spark.sql.parquet.compression.codec","snappy")
      //进行数据的读取，处理分析数据
    //dir/2016-10-01_06_p1_invalid.1475274123982.log
    val lines = sc.textFile(inputPath)

    val rowRDD = lines.map(_.split(",",-1)).filter(_.length>=85).map(
      arr => {
        Row(
        arr(0),
        Utils2Type.toInt(arr(1)),
        Utils2Type.toInt(arr(2)),
        Utils2Type.toInt(arr(3)),
        Utils2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Utils2Type.toInt(arr(7)),
        Utils2Type.toInt(arr(8)),
        Utils2Type.toDouble(arr(9)),
        Utils2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Utils2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Utils2Type.toInt(arr(20)),
        Utils2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Utils2Type.toInt(arr(26)),
        arr(27),
        Utils2Type.toInt(arr(28)),
        arr(29),
        Utils2Type.toInt(arr(30)),
        Utils2Type.toInt(arr(31)),
        Utils2Type.toInt(arr(32)),
        arr(33),
        Utils2Type.toInt(arr(34)),
        Utils2Type.toInt(arr(35)),
        Utils2Type.toInt(arr(36)),
        arr(37),
        Utils2Type.toInt(arr(38)),
        Utils2Type.toInt(arr(39)),
        Utils2Type.toDouble(arr(40)),
        Utils2Type.toDouble(arr(41)),
        Utils2Type.toInt(arr(42)),
        arr(43),
        Utils2Type.toDouble(arr(44)),
        Utils2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Utils2Type.toInt(arr(57)),
        Utils2Type.toDouble(arr(58)),
        Utils2Type.toInt(arr(59)),
        Utils2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Utils2Type.toInt(arr(73)),
        Utils2Type.toDouble(arr(74)),
        Utils2Type.toDouble(arr(75)),
        Utils2Type.toDouble(arr(76)),
        Utils2Type.toDouble(arr(77)),
        Utils2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Utils2Type.toInt(arr(84))
        )
      }
    )
//构建DF
    val df = sqlcontext.createDataFrame(rowRDD,SchemaUtils.structtype)
    df.write.parquet(outputPath)

    val readqt: DataFrame = sqlcontext.read.parquet(outputPath)
    readqt.createTempView("t_table")

    /**
      * select count(*) ct,provincename,cityname from t_table group by provincename,cityname  ;t1
      */
    val res: DataFrame = sqlcontext.sql("select count(*) ct,provincename,cityname from t_table group by provincename,cityname")
    //res.toJSON.show()

    //res.write.mode(SaveMode.Append).partitionBy("provincename","cityname").save("hdfs://cdh:9000/ad")

    //    sql.write.mode(SaveMode.Append).partitionBy("provincename","cityname").save("hdfs://cdh:9000/ad")
   // res.coalesce(1).write.json("f://bb")
//    res.write.mode(SaveMode.Append).format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/db1")
//      .option("dbtable", "employees1")
//      .option("user", "root")
//      .option("password", "123456")
//      .save()
    sc.stop()
  }
}


package com.Rpt

import java.sql
import java.sql.{DriverManager, ResultSet}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.mysql.jdbc.{Connection, PreparedStatement}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext}
import com.utils.RptUtils

/**
  * 地域分析
  */
object LocationRpt {
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

    import sqlcontext.implicits._
    //获取数据
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
      // 创建三个对应的方法处理九个指标
      val a = RptUtils.request(requestmode, processnode)
      val b = RptUtils.click(requestmode, iseffective)
      val c = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      ((pro, city), a ++ b ::: c)
    })

      //根据key聚合value
    .reduceByKey((list1,list2) => {
      //list((1,1),(2,2),(3,3))
      list1.zip(list2).map(x => x._1+x._2)
    })

    /**
      * ((江西省,宜春市),List(5.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
      * ((湖北省,襄樊市),List(4.0, 4.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
      * ((安徽省,淮北市),List(7.0, 7.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
      */
    //res.collect().toBuffer.foreach(println)

      res.foreachPartition(data2MySql)



//
//    df.createTempView("t_table")
//    sqlcontext.sql("select provincename,cityname," +
//      "sum(case when requestmode==1 and processnode>=1 then 1 else 0 end) as `原始请求数`," +
//      "sum(case when requestmode==1 and processnode>=2 then 1 else 0 end) as `有效请求数量`," +
//      "sum(case when requestmode==1 and processnode=3 then 1 else 0 end) as `满足广广告请求条件的请求数量`," +
//      "sum(case when iseffective==1 and isbilling==1 and isbid==1 then 1 else 0 end) as `竞价次数`," +
//      "sum(case when iseffective==1 and isbilling==1 and iswin!=1 and adorderid!=0 then 1 else 0 end) as `竞价成功数`," +
//      "sum(case when requestmode==2 and iseffective==1 then 1 else 0 end) as `展示数`," +
//      "sum(case when requestmode==3 and iseffective==1 then 1 else 0 end) as `点击数`," +
//      "sum(case when iseffective==1 and isbilling ==1 and iswin==1 then 1 else 0 end)/1000 as `DSP广告消费`," +
//      "sum(case when iseffective==1 and isbilling ==1 and iswin==1 then 1 else 0 end)/1000 as `DSP广告成本`" +
//      "from t_table " +
//      "group by provincename,cityname").show()
////sqlcontext.sql("""""")

  }

  def data2MySql = (it:Iterator[((String,String),List[Double])]) => {
    //创建连接池核心工具类
    val dataSource = new ComboPooledDataSource()
//    //第二步：连接池，url，驱动，账号，密码，初始连接数，最大连接数
//    dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/spark_1");//设置url
//    dataSource.setDriverClass("com.mysql.jdbc.Driver");//设置驱动
//    dataSource.setUser("root");//mysql的账号
//    dataSource.setPassword("123456");//mysql的密码
//    dataSource.setInitialPoolSize(6);//初始连接数，即初始化6个连接
//    dataSource.setMaxPoolSize(50);//最大连接数，即最大的连接数是50
//    dataSource.setMaxIdleTime(60);//最大空闲时间


    //第三步：从连接池对象中获取数据库连接
    val con=dataSource.getConnection()
    val sql="insert into LocationRpt(province, city, 原始请求,有效请求,广告请求,参与竞价数,成功竞价数,展示数,点击数,消费,成本) values(?,?,?,?,?,?,?,?,?,?,?) "

   it.foreach( tup => {
     val ps=con.prepareStatement(sql)

     ps.setString(1,tup._1._1)
     ps.setString(2,tup._1._2)
     ps.setDouble(3,tup._2.head)
     ps.setDouble(4,tup._2(1))
     ps.setDouble(5,tup._2(2))
     ps.setDouble(6,tup._2(3))
     ps.setDouble(7,tup._2(4))
     ps.setDouble(8,tup._2(5))
     ps.setDouble(9,tup._2(6))
     ps.setDouble(10,tup._2(7))
     ps.setDouble(11,tup._2(8))

     ps.executeUpdate()
    // val rs=ps.executeQuery()
   }
    )

   con.close()
  }
}
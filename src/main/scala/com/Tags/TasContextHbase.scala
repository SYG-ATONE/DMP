package com.Tags

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{JedisUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}

/**
  * 上下文标签
  */
object TasContextHbase {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("目录不匹配")
      sys.exit()
    }

    val Array(intputPath, outputPath, dirPath, keywordPath, days) = args
    //创建上下文
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // todo 调用HbaseAPI
    val config: Config = ConfigFactory.load()
    //表名字
    val tbname: String = config.getString("hbase.TableName")
    //创建hadoop任务
    val hdConf: Configuration = spark.sparkContext.hadoopConfiguration
    hdConf.set("hbase.master", config.getString("hbase.master"))
    hdConf.set("hbase.zookeeper.quorum", config.getString("hbase.host"))
    //创建HbaseConnection
    val hbconn: Connection = ConnectionFactory.createConnection(hdConf)


    val admin: Admin = hbconn.getAdmin
    if (!admin.tableExists(TableName.valueOf(tbname))) {
      //创建表操作
      val tableDe = new HTableDescriptor(TableName.valueOf(tbname))
      val colDesc = new HColumnDescriptor("tag")
      tableDe.addFamily(colDesc)
      admin.createTable(tableDe)
      admin.close()
      hbconn.close()
    }
    val jobconf = new JobConf(hdConf)

    //指定输出类型和列表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, tbname)


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

    val id = df.filter(TagUtils.OneUserId)
      .rdd.map(row => {
      val userList = TagUtils.getAllUserId(row)
      (userList, row)
    })
    //过滤符合id的数据,拿出用户
    //构建点的集合
    val  vertiesRDD = id.mapPartitions(x =>x ).flatMap(tp => {
      val jedis = JedisUtils.getConnection()
      map.map(a => {
        jedis.set(a._1, a._2)
      })

      val row = tp._2

      //所有标签
      //接下来通过row数据，按照要求打上所有的标签
      //对广告位类型和广告广告类型名称打标签
      val adList = TagsAd.makeTags(row)
      //App名称打标签
      val appList = TagsApp.makeTags(row, jedis)

      //关键字打标签
      val wordList = TagKeyWord.makeTags(row, bdword)
      //渠道标签
      // val channel = row.getAs[Int]("")
      val dvList = TagDevice.makeTags(row)
      val locationList = Taglocation.makeTags(row)
      //商圈标签
      val business = BusinessTag.makeTags(row)
      jedis.close()
      val AllTag = adList ++ appList ++ wordList ++ dvList ++ locationList ++ business

      // List((String,Int))
      // 保证其中一个点携带者所有标签，同时也保留所有userId
      val VD = tp._1.map((_, 0)) ++ AllTag
      // 处理所有的点集合
      tp._1.map(uId => {
        // 保证一个点携带标签 (uid,vd),(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) (uId.hashCode.toLong, VD) else (uId.hashCode.toLong, List.empty)
      })
    })
    // vertiesRDD.take(50).foreach(println)
    // 构建边的集合
    val edges: RDD[Edge[Int]] = id.flatMap(tp => {
      // A B C : A->B A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
   // edges.take(20).foreach(println)
    // 构建图
    val graph = Graph(vertiesRDD,edges)
    // 取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    // 处理所有的标签和id
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      // 聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      .take(20).foreach(println)




    //    //进行标签的实现
    //    val res = id.rdd.map(row => {
    //      val jedis: Jedis = JedisUtils.getConnection()
    //      //          map.map(a=>{
    //      //            jedis.set(a._1,a._2)
    //      //          })
    //      //取出用户id
    //      val userId = TagUtils.getOneUserId(row)
    //      //接下来通过row数据，按照要求打上所有的标签
    //      //对广告位类型和广告广告类型名称打标签
    //      val adList = TagsAd.makeTags(row)
    //       //App名称打标签
    //      val appList: List[(String, Int)] = TagsApp.makeTags(row, jedis)
    //
    //      //关键字打标签
    //      val wordList = TagKeyWord.makeTags(row, bdword)
    //
    //      //渠道标签
    //      // val channel = row.getAs[Int]("")
    //      val dvList = TagDevice.makeTags(row)
    //      val locationList = Taglocation.makeTags(row)
    //
    //      //商圈标签
    //      val business = BusinessTag.makeTags(row)
    //
    //      jedis.close()
    //      (userId, adList ++ appList ++ wordList ++ dvList ++ locationList ++ business)
    //    })
    //      .reduceByKey((list1,list2) =>
    //        //对list集合进行拼接  List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
    //        (list1:::list2).groupBy(_._1)
    //          .mapValues(_.foldLeft[Int](0)(_+_._2))
    //          .toList
    //      ).map{
    //      case (userid, userTag) => {
    //        val put = new Put(Bytes.toBytes(userid))
    //        //处理以下标签
    //        val tags: String = userTag.map(tup => tup._1 + "," + tup._2).mkString(",")
    //        put.addImmutable(Bytes.toBytes("tag"),Bytes.toBytes(days),Bytes.toBytes(tags))
    //        (new ImmutableBytesWritable(), put)
    //      }
    //    }.saveAsHadoopDataset(jobconf)
    //    //res.collect().foreach(println)
    spark.close()
  }

}

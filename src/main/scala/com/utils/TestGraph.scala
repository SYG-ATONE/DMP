package com.utils

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestGraph {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val vertexRDD = sc.makeRDD(Seq(

      (1L, ("小红", 20)),
      (3L, ("小明", 33)),
      (5L, ("小七", 20)),
      (7L, ("小王", 60)),
      (9L, ("小李", 20)),
      (11L, ("小美", 30))
     // (13L, ("小花", 0))
    ))

    val egde: RDD[Edge[Int]] = sc.makeRDD(Seq(

      Edge(1L,13L,0),
      Edge(9L, 13L, 0),
      Edge(11L, 13L, 0),
      Edge(3L, 13L, 0),
      Edge(7L, 5L, 0)
    ))
    egde
    val grap = Graph(vertexRDD,egde)
    val vertices = grap.connectedComponents().vertices
    val res: RDD[(VertexId, List[Any])] = vertices.join(vertexRDD).map {
      case (userId, (conId, (name, age))) => {
        (conId, List(name, age))
      }
    }.reduceByKey(_ ++ _)
//    def data2MySql =
//      (it:Iterator[(VertexId, List[Any])]) => {
//        val dataSource = new ComboPooledDataSource()
//        //第三步：从连接池对象中获取数据库连接
//        val con=dataSource.getConnection()
//        val sql = "insert into spark_2 (name,age) values (?,?)"
//        it.foreach(tup =>{
//          val ps=con.prepareStatement(sql)
//          ps.setString(1,tup._2.toString())
//          ps.setString(2,tup._2.toString())
//          ps.executeUpdate()
//        })
//      }

    //.foreach(println)

  }
}

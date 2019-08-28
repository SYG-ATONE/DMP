
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{GenTraversableOnce, mutable}
import scala.collection.mutable.ListBuffer
//1、按照pois，分类businessarea，并统计每个businessarea的总数。
object TagsContext2 {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val rdd: RDD[String] = sc.textFile("dir/json.txt")

    val buff: mutable.Buffer[String] = rdd.collect().toBuffer


    var list: List[List[String]] = List()
    for(i <- buff){                   //0 to buff.length
      val str: String = i.toString         //buff(i)

      //解析json串
      val jsonparse: JSONObject = JSON.parseObject(str)

      //判断状态
      val status = jsonparse.getIntValue("status")
      if(status == 0) return ""

      // 接下来解析内部json串，判断每个key的value都不能为空
      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null || poisArray.isEmpty) return null


      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      // 循环输出
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      val list1: List[String] = buffer.toList

      list:+=list1
    }

    val res = list.flatMap(x => x)
      .filter(x => x != "").map(x => (x, 1))
      .groupBy(x => x._1)
      .mapValues(x => x.size)


    res.foreach(println)

  }
}

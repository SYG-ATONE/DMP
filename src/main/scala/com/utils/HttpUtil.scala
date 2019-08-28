package com.utils

import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object HttpUtil {

  //get请求
def get(url:String):String={
  val client = HttpClients.createDefault()
  val get = new HttpGet(url)
  val response = client.execute(get)
  //获取返回结果
  EntityUtils.toString(response.getEntity,"UTF-8")
}
}

package com.utils

/**
  * 指标方法
  */
object RptUtils {
  //此方法处理请求数
  def request(requestmode: Int, processndoe: Int): List[Double] = {
    var q = 0
    var r = 0
    var t = 0
    if (requestmode == 1 && processndoe >= 1) {
      q += 1
    }
    if (requestmode == 1 && processndoe >= 2){
    r+=1
    }
  if (requestmode == 1 && processndoe == 3) t+=1
    List[Double](q,r,t)
  }


  //此方法处理展示点击数
  def click(requestmode: Int, iseffective: Int): List[Double] = {
var m = 0
    var n = 0
    if (requestmode==2 && iseffective==1)m+=1
    if (requestmode==3 && iseffective==1)n+=1
    List[Double](m,n)
  }

  /**
    * 参与竞价数
    * 竞价成功数
    * DSP广告消费
    * DSP广告成本
    */
  //此方法处理竞价操作
  def Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, WinPrice: Double, adpayment: Double): List[Double] = {

    var a = 0.0
    var b = 0.0
    var c = 0.0
    var d = 0.0
    if (iseffective==1 && isbilling==1 && isbid==1)a+=1
    if (iseffective==1 && isbilling==1 && iswin==1 && adorderid!=1)b+=1
    if (iseffective==1 && isbilling==1 && iswin==1){
    c+=WinPrice/1000.0
      d+=adorderid/1000.0
    }
    List[Double](a,b,c,d)
  }


  }

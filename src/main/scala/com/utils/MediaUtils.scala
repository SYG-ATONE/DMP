package com.utils

object MediaUtils {

  //原始请求数 有效请求  广告请求
  def AA(requestmode: Int, processnode: Int): List[Double] = {
    var a = 0
    var b = 0
    var c = 0
    if (requestmode == 1 && processnode >= 1) a += 1
    if (requestmode == 1 && processnode >= 2) b += 1
    if (requestmode == 1 && processnode == 3) c += 1
    List[Double](a, b, c)
  }

  //展示数量  点击数量
  def BB(requestmode: Int, iseffective: Int): List[Double] = {
    var a = 0
    var b = 0
    if (requestmode == 2 && iseffective == 1) a += 1
    if (requestmode == 3 && iseffective == 1) b += 1
    List[Double](a, b)
  }

  def CC(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, WinPrice: Double, adpayment: Double): List[Double] = {

    var a = 0.0
    var b = 0.0
    var c = 0.0
    var d = 0.0
    var e = 0.0
    if (iseffective == 1 && isbilling == 1 && isbid == 1) a += 1
    if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 1) b += 1
    if (iseffective == 1 && isbilling == 1 && iswin == 1) {
      c += WinPrice / 1000.0
      d += adorderid / 1000.0
      try {
        e = b / a
      } catch {
        case e: ArithmeticException => println(e)
      }
    }
    List[Double](a, b, c, d, e)
  }
}

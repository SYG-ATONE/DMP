package com.utils
import scala.util.control.Breaks
object QuickSort {

  def main(args: Array[String]): Unit = {
    val arr = Array(10, 11, 2, -1, 3)

    println(quickSort(0, arr.length - 1, arr).toBuffer)
  }
  def quickSort(left: Int, right: Int, arr: Array[Int]): Array[Int] = {

    var l: Int = left
    var r: Int = right
    var pivot = arr((left + right) / 2)
    var temp = 0

    Breaks.breakable {
      while (l < r) {

        //从左点向右遍历，直到找到比中间值大的
        while (arr(l) < pivot) {
          l += 1
        }

        //从右点向左遍历，直到找到比中间值小的
        while (arr(r) > pivot) {
          r -= 1
        }

        //判断是否已经越过中间值
        if (l >= r) {
         Breaks.break()
        }

        //交换数据
        temp = arr(l)
        arr(l) = arr(r)
        arr(r) = temp
      }
    }

    if (l == r) {
      l += 1
      r -= 1
    }

    //向左递归
    if (left < r) {
      quickSort(left, r, arr)
    }

    //向右递归
    if (right > l) {
      quickSort(l, right, arr)
    }
arr
  }

}

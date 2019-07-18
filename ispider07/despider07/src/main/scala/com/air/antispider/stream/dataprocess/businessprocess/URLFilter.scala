package com.air.antispider.stream.dataprocess.businessprocess

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 使用广播变量,实现URL过滤功能
  */
object URLFilter {
  /**
    *
    * @param message 原始数据
    * @param filterRulList 过滤规则
    */
  def filterURL(message: String, filterRulList: ArrayBuffer[String]): Boolean = {
      //看当前的message是否匹配filterRuleList
      //先取出message中的URL
      var url = ""
      val arr: Array[String] = message.split("#CS#")
      if (arr.length > 1) {
        val arrTemp: Array[String] = arr(1).split(" ")
        if (arrTemp.length > 1) {
          url = arrTemp(1)
        }
      }
      //判断是否能取出URL,
      if (StringUtils.isBlank(url)) {
       return false
      }
      //遍历filterRulList
      for (filterRule <- filterRulList) {
        if (url.matches(filterRule)) {
          return false
        }
      }
      //如果整个集合都遍历完了,还没有return,那肯定是没有一个能匹配上
      return true
    }
}

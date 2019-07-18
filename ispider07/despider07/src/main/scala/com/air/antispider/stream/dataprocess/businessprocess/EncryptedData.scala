package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.{Matcher, Pattern}

import com.air.antispider.stream.common.util.decode.MD5
import org.apache.spark.rdd.RDD

/**
  * 对用户的敏感信息进行加密操作
  */
object EncryptedData {
  /**
    * 加密身份证号
    * @param encryptedPhoneRDD
    * @return
    */
  def encryptedID(encryptedPhoneRDD: RDD[String]): RDD[String] = {
    //如何找到手机号
    encryptedPhoneRDD.map(message => {
      //创建加密对象
      val md5 = new MD5
      //找message中的手机号
      //可以使用正则表达式来找
      val pattern: Pattern = Pattern.compile("(\\d{18})|(\\d{17}(\\d|X|x))|(\\d{15})")
      //使用正则对象,对message进行匹配,matcher是匹配结果
      val matcher: Matcher = pattern.matcher(message)
      var tempMessage = message
      //      while (iterator.hasNext()) {
      //        iterator.next()
      //      }
      //循环结果,看有没有匹配到的数据
      while (matcher.find()) {
        //取出匹配结果
        val id: String = matcher.group()
        //加密/替换
        val encryptedID: String = md5.getMD5ofStr(id)
        tempMessage = tempMessage.replace(id, encryptedID)
      }
      //返回加密之后的数据
      tempMessage
    })
  }

  //手机号加密
  def encryptedPhone(filterRDD: RDD[String]): RDD[String] = {

    //如何找到手机号
    filterRDD.map(message => {
      //创建加密对象
      val md5 = new MD5
      //找message中的手机号
      //可以使用正则表达式来找
      val pattern: Pattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")
      //使用正则对象,对message进行匹配,matcher是匹配结果
      val matcher: Matcher = pattern.matcher(message)
      var tempMessage = message
//      while (iterator.hasNext()) {
//        iterator.next()
//      }
      //循环结果,看有没有匹配到的数据
      while (matcher.find()) {
        //取出匹配结果
        val phone: String = matcher.group()
        //加密/替换
        val encryptedPhone: String = md5.getMD5ofStr(phone)
        tempMessage = tempMessage.replace(phone, encryptedPhone)
      }
      //返回加密之后的数据
      tempMessage
    })



  }

}

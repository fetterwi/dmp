package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.RequestType
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}
import com.air.antispider.stream.dataprocess.constants.FlightTypeEnum.FlightTypeEnum

import scala.collection.mutable.ArrayBuffer

object RequestTypeClassifier {
  /**
    * 对请求的分类进行判断
    * @param request
    * @param classifyRuleMap
    * @return 用户的请求分类信息(国内,查询)
    */
  def requestTypeClassifier(request: String, classifyRuleMap: Map[String, ArrayBuffer[String]]): RequestType = {
    //取出分类集合中的数据
    val nationalQueryList: ArrayBuffer[String] = classifyRuleMap.getOrElse("nationalQueryList", null)
    val nationalBookList: ArrayBuffer[String] = classifyRuleMap.getOrElse("nationalBookList", null)
    val internationalQueryList: ArrayBuffer[String] = classifyRuleMap.getOrElse("internationalQueryList", null)
    val internationalBookList: ArrayBuffer[String] = classifyRuleMap.getOrElse("internationalBookList", null)

    //变量这4个集合,看当前的request在哪个集合中匹配
    //国内查询
    if (nationalQueryList != null) {
      //      fira code
      for (expression <- nationalQueryList) {
        //判断当前请求的URL是否和本正则匹配
        if (request.matches(expression)) {
          return RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Query)
        }
      }
    }
    //国内预定
    if (nationalBookList != null) {
      //      fira code
      for (expression <- nationalBookList) {
        //判断当前请求的URL是否和本正则匹配
        if (request.matches(expression)) {
          return RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Book)
        }
      }
    }
    //国际查询
    if (internationalQueryList != null) {
      //      fira code
      for (expression <- internationalQueryList) {
        //判断当前请求的URL是否和本正则匹配
        if (request.matches(expression)) {
          return RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Query)
        }
      }
    }
    //国际预定
    if (internationalBookList != null) {
      //      fira code
      for (expression <- internationalBookList) {
        //判断当前请求的URL是否和本正则匹配
        if (request.matches(expression)) {
          return RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Book)
        }
      }
    }

    //如果上面没有任何一个匹配上,那么返回一个默认值
    return RequestType(FlightTypeEnum.Other, BehaviorTypeEnum.Other)
  }

}

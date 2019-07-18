package com.air.antispider.stream.dataprocess.businessprocess

import java.util.regex.{Matcher, Pattern}

import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum

/**
  * 往返信息打标签
  */
object TravelTypeClassifier {

  def travelTypeClassifier(httpReferrer: String): TravelTypeEnum = {
    val pattern: Pattern = Pattern.compile("(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])")
    val matcher: Matcher = pattern.matcher(httpReferrer)

    //创建一个计数器
    var num = 0

    //调用find方法的时候,游标会自动向下
    while (matcher.find()) {
      num = num + 1
    }

    if (num == 1) {
      //是单程
      return TravelTypeEnum.OneWay
    } else if (num == 2) {
      //是往返
      return TravelTypeEnum.RoundTrip
    } else {
      //不知道啊
      return TravelTypeEnum.Unknown
    }
  }

}

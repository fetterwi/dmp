package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.{BookRequestData, CoreRequestParams, ProcessedData, QueryRequestData, RequestType}
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum

object DataPackage {
  /**
    * 对下方散乱的数据,进行封装,封装为ProcessedData
    * @param source
    * @param requestMethod
    * @param request
    * @param remoteAddr
    * @param httpUserAgent
    * @param timeIso8601
    * @param serverAddr
    * @param highFrqIPGroup
    * @param requestType
    * @param travelType
    * @param cookieValue_JSESSIONID
    * @param cookieValue_USERID
    * @param queryParams
    * @param bookParams
    * @param httpReferrer
    * @return
    */
  def dataPackage(sourceData: String,
                  requestMethod: String,
                  request: String,
                  remoteAddr: String,
                  httpUserAgent: String,
                  timeIso8601: String,
                  serverAddr: String,
                  highFrqIPGroup: Boolean,
                  requestType: RequestType,
                  travelType: TravelTypeEnum,
                  cookieValue_JSESSIONID: String,
                  cookieValue_USERID: String,
                  queryParams: Option[QueryRequestData],
                  bookParams: Option[BookRequestData],
                  httpReferrer: String): ProcessedData = {

    //因为创建ProcessedData的时候,还需要核心请求参数,
    //但这些参数在queryParams/bookParams中

    //定义出发时间/始发地/目的地等参数
    var flightDate: String = ""
    //出发地
    var depcity: String = ""
    //目的地
    var arrcity: String = ""


   //看查询请求参数中有没有值
    queryParams match {
      //Option有值的情况,queryData:如果有值,就使用此变量操作
      case Some(x) =>
        flightDate = x.flightDate
        depcity = x.depCity
        arrcity = x.arrCity
      //None:没有值
      case None =>
        //如果查询请求参数没有值,就去预定请求参数中获取
        bookParams match {
          //Option有值的情况,queryData:如果有值,就使用此变量操作
          case Some(bookData) =>
            //为了确保安全,需要加上长度判断,只有长度大于0才能这样取值
            flightDate = bookData.flightDate.mkString
            depcity = bookData.depCity.mkString
            arrcity = bookData.arrCity.mkString
          //None:没有值
          case None =>
        }
    }



    //创建核心请求参数
    val requestParams = CoreRequestParams(flightDate, depcity, arrcity)

    ProcessedData(
      sourceData,
      requestMethod,
      request,
      remoteAddr,
      httpUserAgent,
      timeIso8601,
      serverAddr,
      highFrqIPGroup,
      requestType,
      travelType,
      requestParams,
      cookieValue_JSESSIONID,
      cookieValue_USERID,
      queryParams,
      bookParams,
      httpReferrer)
  }

}

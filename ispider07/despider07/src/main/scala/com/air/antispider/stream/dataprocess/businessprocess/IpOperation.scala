package com.air.antispider.stream.dataprocess.businessprocess

import scala.collection.mutable.ArrayBuffer

object IpOperation {
  /**
    * 判断当前客户端IP是否是高频IP
    * @param remoteAddr
    * @param blackIPList
    * @return
    */
  def operationIP(remoteAddr: String, blackIPList: ArrayBuffer[String]): Boolean = {
    //遍历blackIPList,判断remoteAddr在集合中是否存在
    for (blackIP <- blackIPList) {
      if (blackIP.equals(remoteAddr)){
        //如果相等,当前IP是高频IP
        return true
      }
    }
    return false
  }
}

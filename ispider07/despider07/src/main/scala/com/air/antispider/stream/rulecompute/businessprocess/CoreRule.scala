package com.air.antispider.stream.rulecompute.businessprocess

import java.util

import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 按照不同的维度进行计算的工具类
  */
object CoreRule {
  /**
    * 计算IP5分钟携带不同Cookie的数量
    * @param processedDataRDD
    * @param criticalPagesList
    * @return
    */
  def ipCookieCount(processedDataRDD: RDD[ProcessedData], criticalPagesList: ArrayBuffer[String]): collection.Map[String, Int] = {
    //先过滤出关键页面
    processedDataRDD
      //过滤
      .filter(processedData => {
      val url: String = processedData.request
      //定义访问次数,默认为0次
      var count = 0
      for (criticalPages <- criticalPagesList) {
        if (url.matches(criticalPages)) {
          //如果匹配上,代表访问了1次关键页面
          count = 1
        }
      }
      //如果count == 1,代表当前访问的是关键页面,返回true
      if (count == 0) {
        false
      } else {
        true
      }
    })
      //(ip , jSessionID)
      .map(line => {
        val ip: String = line.remoteAddr
        //SessionID
        val sessionID: String = line.cookieValue_JSESSIONID
        (ip, sessionID)
      })
        .groupByKey()
        //(ip, (sID1, sID2, sID1))
        .map(line => {
        val ip: String = line._1
        val sourceSessionIDs: Iterable[String] = line._2
        //定义Set集合实现去重
        var set = Set[String]()
        //循环,去重
        for (sessionID <- sourceSessionIDs) {
          set += sessionID
        }
        (ip, set.size)
      })
        .collectAsMap()
  }


  /**
    * 计算IP5分钟查询不同航班的次数
    *
    * @param processedDataRDD
    * @return
    */
  def ipCityCount(processedDataRDD: RDD[ProcessedData]): collection.Map[String, Int] = {
    //(ip , 出发地->目的地)
    processedDataRDD.map(line => {
      val ip: String = line.remoteAddr
      //出发地
      val depcity: String = line.requestParams.depcity
      //目的地
      val arrcity: String = line.requestParams.arrcity
      (ip, depcity + "->" + arrcity)
    })
      .groupByKey()
      //(ip, 不同城市的次数)
      .map(line => {
        val ip: String = line._1
        val sourceCitys: Iterable[String] = line._2
        //定义Set集合实现去重
        var set = Set[String]()
        //循环,去重
        for (city <- sourceCitys) {
          set += city
        }
        (ip, set.size)
      })
      .collectAsMap()
  }

  /**
    * 计算IP5分钟访问关键页面最小时间间隔小于预设值的次数
    *
    * @param processedDataRDD
    * @param criticalPagesList
    * @return
    */
  def ipCriticalPagesMinNumCount(processedDataRDD: RDD[ProcessedData], criticalPagesList: ArrayBuffer[String]): collection.Map[(String, String), Int] = {
    //先过滤出关键页面
    processedDataRDD
      //过滤
      .filter(processedData => {
      val url: String = processedData.request
      //定义访问次数,默认为0次
      var count = 0
      for (criticalPages <- criticalPagesList) {
        if (url.matches(criticalPages)) {
          //如果匹配上,代表访问了1次关键页面
          count = 1
        }
      }
      //如果count == 1,代表当前访问的是关键页面,返回true
      if (count == 0) {
        false
      } else {
        true
      }
    })
      //转换,获取((IP, URL),时间戳)
      .map(processedData => {
      val ip: String = processedData.remoteAddr
      val url: String = processedData.request
      val time: String = processedData.timeIso8601
      //time的格式2019-06-29T08:46:56+08:00
      val timeStamp: Long = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss").parse(time).getTime
      ((ip, url), timeStamp)
    })
      //分组((IP, URL),(时间1,时间2,时间3...))
      .groupByKey()
      //转换,为了获取(IP,最小时间差)
      .map(line => {
      val key: (String, String) = line._1
      //封装所有时间的迭代器对象
      val sourceData: Iterable[Long] = line._2
      //将迭代器对象转换为Array
      val sourceArray: Array[Long] = sourceData.toArray
      //将原始数据进行排序
      util.Arrays.sort(sourceArray)
      //定义一个用于存储差值的集合
      var resultArray = new ArrayBuffer[Long]()
      for (i <- 0 until sourceArray.size - 1) {
        //当前元素
        val currentTime: Long = sourceArray(i)
        //下一个元素
        val nexTime: Long = sourceArray(i + 1)
        val result = nexTime - currentTime
        //将小于预设值的差值存入集合(此处直接写死5秒钟)
        if (result < 5000) {
          resultArray += result
        }
      }
      //返回((ip, url), 次数)
      (key, resultArray.size)
    })
      .collectAsMap()
  }

  /**
    * 计算IP5分钟访问关键页面最小时间间隔
    *
    * @param processedDataRDD
    * @param criticalPagesList
    * @return
    */
  def ipCriticalPagesMinTimeCount(processedDataRDD: RDD[ProcessedData], criticalPagesList: ArrayBuffer[String]): collection.Map[String, Long] = {
    //先过滤出关键页面
    processedDataRDD
      //过滤
      .filter(processedData => {
      val url: String = processedData.request
      //定义访问次数,默认为0次
      var count = 0
      for (criticalPages <- criticalPagesList) {
        if (url.matches(criticalPages)) {
          //如果匹配上,代表访问了1次关键页面
          count = 1
        }
      }
      //如果count == 1,代表当前访问的是关键页面,返回true
      if (count == 0) {
        false
      } else {
        true
      }
    })
      //转换,获取(ip,时间戳)
      .map(processedData => {
        val ip: String = processedData.remoteAddr
        val time: String = processedData.timeIso8601
        //time的格式2019-06-29T08:46:56+08:00
        val timeStamp: Long = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss").parse(time).getTime
        (ip, timeStamp)
      })
      //分组(ip,(时间1,时间2,时间3...))
      .groupByKey()
      //转换,为了获取(IP,最小时间差)
      .map(line => {
        val ip: String = line._1
        //封装所有时间的迭代器对象
        val sourceData: Iterable[Long] = line._2
        //将迭代器对象转换为Array
        val sourceArray: Array[Long] = sourceData.toArray
        //将原始数据进行排序
        util.Arrays.sort(sourceArray)
        //定义一个用于存储差值的集合
        var resultArray = new ArrayBuffer[Long]()
        for (i <- 0 until sourceArray.size - 1) {
          //当前元素
          val currentTime: Long = sourceArray(i)
          //下一个元素
          val nexTime: Long = sourceArray(i + 1)
          val result = nexTime - currentTime
          //将差值存入集合
          resultArray += result
        }
        //将差值结果进行排序
        val array: Array[Long] = resultArray.toArray
        util.Arrays.sort(array)
        (ip, array(0))
      })
      //采集数据
      .collectAsMap()
  }

  /**
    * 计算IP5分钟携带不同UA的个数
    *
    * @param processedDataRDD
    * @return
    */
  def ipUACount(processedDataRDD: RDD[ProcessedData]): collection.Map[String, Int] = {
    //将processedData转换为(ip, ua)的格式
    val mapData: RDD[(String, String)] = processedDataRDD.map(processedData => {
      val ip: String = processedData.remoteAddr
      val ua: String = processedData.httpUserAgent
      (ip, ua)
    })
    //(ip, ua) => (ip, (ua1, ua2, ua1))的格式
    val groupRDD: RDD[(String, Iterable[String])] = mapData.groupByKey()
    //将(ip, (ua1, ua2, ua1))的格式 转换为 (ip, 次数)的格式
    groupRDD.map(line => {
      val ip: String = line._1
      val sourceData: Iterable[String] = line._2
      //创建一个Set集合,将原始的数据放入集合中,去重
      var set = Set[String]()
      for (ua <- sourceData) {
        //将ua放入set集合
        set += ua
      }
      (ip, set.size)
    })
      .collectAsMap()

  }

  /**
    * 计算IP访问关键页面的次数
    *
    * @param processedDataRDD
    * @param criticalPagesList
    * @return
    */
  def ipCriticalPagesCount(processedDataRDD: RDD[ProcessedData], criticalPagesList: ArrayBuffer[String]): collection.Map[String, Int] = {
    processedDataRDD.map(processedData => {
      val ip: String = processedData.remoteAddr
      val url: String = processedData.request
      //定义访问次数,默认为0次
      var count = 0
      for (criticalPages <- criticalPagesList) {
        if (url.matches(criticalPages)) {
          //如果匹配上,代表访问了1次关键页面
          count = 1
        }
      }
      (ip, count)
    })
      //累加
      .reduceByKey(_ + _)
      //采集数据
      .collectAsMap()
  }

  /**
    * 计算IP5分钟访问量
    *
    * @param processedDataRDD
    * @return
    */
  def ipCount(processedDataRDD: RDD[ProcessedData]): collection.Map[String, Int] = {
    processedDataRDD.map(processedData => {
      val ip: String = processedData.remoteAddr
      //(ip, 次数)
      (ip, 1)
    })
      //累加
      .reduceByKey(_ + _)
      //采集数据
      .collectAsMap()
  }

  /**
    * IP段指标计算
    *
    * @param processedDataRDD
    */
  def ipBlockCount(processedDataRDD: RDD[ProcessedData]): collection.Map[String, Int] = {
    val mapRDD: RDD[(String, Int)] = processedDataRDD.map(processedData => {
      //获取客户端IP 192.168.80.81
      val ip: String = processedData.remoteAddr
      //获取IP的前2位, 192.168
      val arr: Array[String] = ip.split("\\.")
      if (arr.length == 4) {
        //代表这是一个完整的IP
        val ipBlock = arr(0) + "." + arr(1)
        //(ip段, 1)
        (ipBlock, 1)
      } else {
        ("", 1)
      }
    })
    //按照IP段进行分组,聚合计算
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey((x, y) => x + y)
    //将结果采集为Map类型返回
    resultRDD.collectAsMap()
  }

}

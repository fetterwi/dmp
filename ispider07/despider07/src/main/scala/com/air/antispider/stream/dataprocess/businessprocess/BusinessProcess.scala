package com.air.antispider.stream.dataprocess.businessprocess

import java.util.Date

import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

/**
  * 链路统计功能
  */
object BusinessProcess {
  def linkCount(messageRDD: RDD[String]): collection.Map[String, Int] = {
    //信息采集量
    val serverCountRDD: RDD[(String, Int)] = messageRDD.map(message => {
      val arr: Array[String] = message.split("#CS#")
      if (arr.length > 9) {
        //有数据
        val serverIP = arr(9)
        //(ip,1次)
        (serverIP, 1)
      } else {
        ("", 1)
      }
    })
      //按照Key进行累加操作
      .reduceByKey(_ + _)


    //当前活跃连接数
    val activeNumRDD: RDD[(String, Int)] = messageRDD.map(message => {
      val arr: Array[String] = message.split("#CS#")
      if (arr.length > 11) {
        //取IP
        val serverIP = arr(9)
        //取本IP的活跃连接数量
        val activeNum = arr(11)
        //(ip,1次)
        (serverIP, activeNum.toInt)
      } else {
        ("", 1)
      }
    })
      //舍弃一个值,主需要一个活跃连接数就ok了
      .reduceByKey((x, y) => y)


    //进行数据展示

    //通过跟踪java代码,发现我们需要封装一个json数据,存入Redis中,让前端进行数据展示
    if (!serverCountRDD.isEmpty() && !activeNumRDD.isEmpty()) {
      //如果数据不为空,开始数据处理
      //将RDD的结果转换为Map
      val serversCountMap: collection.Map[String, Int] = serverCountRDD.collectAsMap()
      val activeNumMap: collection.Map[String, Int] = activeNumRDD.collectAsMap()

      val map = Map[String, collection.Map[String, Int]](
        "serversCountMap" -> serversCountMap,
        "activeNumMap" -> activeNumMap
      )

      //将map转换为JSON
      val jsonData: String = Json(DefaultFormats).write(map)

      //将jsonData存入Redis中
      //获取Redis连接
      val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster
      //存入数据
      //使用CSANTI_MONITOR_LP + 时间戳   格式来作为Key
      val key: String = PropertiesUtil.getStringByKey("cluster.key.monitor.linkProcess", "jedisConfig.properties") + new Date().getTime
      val ex: Int = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt
      //当前数据是以天为单位进行存储的,所以有效时间,设置为1天就行了
//      jedis.set(key, jsonData)
      //设置超时时间为2分钟
      jedis.setex(key, ex, jsonData)
    }


    //为了外面做Spark性能监控,返回当前处理的数据采集结果信息
    serverCountRDD.collectAsMap()
  }

}

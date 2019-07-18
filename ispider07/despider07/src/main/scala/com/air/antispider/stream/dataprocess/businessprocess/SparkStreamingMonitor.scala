package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.spark.SparkMetricsUtils
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

object SparkStreamingMonitor {
  def queryMonitor(sc: SparkContext, line: RDD[String]): Unit = {
    //1. 获取到Spark的状态信息
    /*
    //在项目上线后,使用下方的方式获取URL
        //监控数据获取
        val sparkDriverHost =
          sc.getConf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY _URI_BASES")
        //在 yarn 上运行的监控数据 json 路径
        val url = s"${sparkDriverHost}/metrics/json"
        */
    val url = "http://localhost:4041/metrics/json/"
    val sparkDataInfo: JSONObject = SparkMetricsUtils.getMetricsJson(url)
    val gaugesObj: JSONObject = sparkDataInfo.getJSONObject("gauges")

    //获取应用ID和应用名称,用来构建json中的key
    val id: String = sc.applicationId
    val appName: String = sc.appName
    //local-1561617727065.driver.DataProcessApp.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime
    val startKey = id + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val endKey = id + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"

    val startTime = gaugesObj.getJSONObject(startKey) //{"value": 1561617812011}
      .getLong("value")
    val endTime = gaugesObj.getJSONObject(endKey) //{"value": 1561617812011}
      .getLong("value")
    //将结束时间进行格式化yyyy-MM-dd HH:mm:ss,注意,web平台使用的是24小时制,所以此处需要使用HH
    val endTimeStr: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(endTime)

    //2. 计算时间差
    val costTime = endTime - startTime

    //3. 根据时间差计算数据处理速度,速度= 数量/时间
    //获取处理的数量
    val count: Double = line.count().toDouble
    //计算处理速度
    var countPer: Double = 0.0
    if (costTime != 0) {
      countPer = count / costTime
    }


    //4. 交给JavaWeb进行结果展示
    //    private String costTime;
    //    private Map<String,Object> serversCountMap;
    //    private String applicationId;
    //    private String applicationUniqueName;
    //    private String countPerMillis;
    //    private String endTime;
    //    private String sourceCount;
    //对serversCountMap进行转换,转换为JSON

    //根据web平台的代码,发现需要存入Redis中
    val message = Map[String, Any](
      "costTime" -> costTime.toString, //时间差
      "applicationId" -> id, //应用ID
      "applicationUniqueName" -> appName, //应用名称
      "countPerMillis" -> countPer.toString,//计算速度
      "endTime" -> endTimeStr, //结束时间:2019-06-27 15:44:32
      "sourceCount" -> count.toString, //数据的数量
      "serversCountMap" -> Map[String, String]() //数据采集信息,实时计算不涉及到链路统计,给个空map
    )

    //将message转换为json
    val messageJson: String = Json(DefaultFormats).write(message)

    //将messageJson发送到Kafka

    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster
    //存入Redis的Key.CSANTI_MONITOR_DP + 时间戳
    val key = PropertiesUtil.getStringByKey("cluster.key.monitor.query", "jedisConfig.properties") + System.currentTimeMillis()
    val ex = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt
    jedis.setex(key, ex, messageJson)


    //如果需要最后一批数据,那么可以使用下面的方式,
    val lastKey = PropertiesUtil.getStringByKey("cluster.key.monitor.query", "jedisConfig.properties") + "_LAST"
    jedis.set(lastKey, messageJson)

  }

  /**
    * Spark性能监控,
    *
    * @param sc
    * @param processedDataRDD
    * @param serversCountMap
    */
  def streamMonitor(sc: SparkContext, processedDataRDD: RDD[ProcessedData], serversCountMap: collection.Map[String, Int]) = {

    //1. 获取到Spark的状态信息
    /*
    //在项目上线后,使用下方的方式获取URL
        //监控数据获取
        val sparkDriverHost =
          sc.getConf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY _URI_BASES")
        //在 yarn 上运行的监控数据 json 路径
        val url = s"${sparkDriverHost}/metrics/json"
        */
    val url = "http://localhost:4040/metrics/json/"
    val sparkDataInfo: JSONObject = SparkMetricsUtils.getMetricsJson(url)
    val gaugesObj: JSONObject = sparkDataInfo.getJSONObject("gauges")

    //获取应用ID和应用名称,用来构建json中的key
    val id: String = sc.applicationId
    val appName: String = sc.appName
    //local-1561617727065.driver.DataProcessApp.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime
    val startKey = id + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val endKey = id + ".driver." + appName + ".StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"

    val startTime = gaugesObj.getJSONObject(startKey) //{"value": 1561617812011}
      .getLong("value")
    val endTime = gaugesObj.getJSONObject(endKey) //{"value": 1561617812011}
      .getLong("value")
    //将结束时间进行格式化yyyy-MM-dd HH:mm:ss,注意,web平台使用的是24小时制,所以此处需要使用HH
    val endTimeStr: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(endTime)

    //2. 计算时间差
    val costTime = endTime - startTime

    //3. 根据时间差计算数据处理速度,速度= 数量/时间
    //获取处理的数量
    val count: Long = processedDataRDD.count()
    //计算处理速度
    var countPer = 0.0
    if (costTime != 0) {
      countPer = count / costTime
    }


    //4. 交给JavaWeb进行结果展示
    //    private String costTime;
    //    private Map<String,Object> serversCountMap;
    //    private String applicationId;
    //    private String applicationUniqueName;
    //    private String countPerMillis;
    //    private String endTime;
    //    private String sourceCount;
    //对serversCountMap进行转换,转换为JSON
    val serversCountMapJson: String = Json(DefaultFormats).write(serversCountMap)

    //根据web平台的代码,发现需要存入Redis中
    val message = Map[String, Any](
      "costTime" -> costTime.toString, //时间差
      "applicationId" -> id, //应用ID
      "applicationUniqueName" -> appName, //应用名称
      "countPerMillis" -> countPer.toString,//计算速度
      "endTime" -> endTimeStr, //结束时间:2019-06-27 15:44:32
      "sourceCount" -> count.toString, //数据的数量
      "serversCountMap" -> serversCountMap //数据采集信息
    )

   //将message转换为json
   val messageJson: String = Json(DefaultFormats).write(message)

    //将messageJson发送到Kafka

    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster
    //存入Redis的Key.CSANTI_MONITOR_DP + 时间戳
    val key = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + System.currentTimeMillis()
    val ex = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt
    jedis.setex(key, ex, messageJson)


    //如果需要最后一批数据,那么可以使用下面的方式,
    val lastKey = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + "_LAST"
    jedis.set(lastKey, messageJson)

  }

}

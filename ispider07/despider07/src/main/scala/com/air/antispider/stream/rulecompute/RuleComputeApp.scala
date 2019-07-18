package com.air.antispider.stream.rulecompute

import java.text.SimpleDateFormat
import java.util.Date

import com.air.antispider.stream.common.bean.{FlowCollocation, ProcessedData}
import com.air.antispider.stream.common.util.HDFS.{BlackListToHDFS, BlackListToRedis}
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.kafka.KafkaOffsetUtil
import com.air.antispider.stream.dataprocess.businessprocess.{AnalyzeRuleDB, SparkStreamingMonitor}
import com.air.antispider.stream.rulecompute.bean.{AntiCalculateResult, FlowScoreResult}
import com.air.antispider.stream.rulecompute.businessprocess.{CoreRule, QueryDataPackage, RuleUtil}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * 黑名单实时计算主程序
  */
object RuleComputeApp {




  def main(args: Array[String]): Unit = {
    //创建Spark执行环境
    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //创建Spark配置对象
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RuleComputeApp")
      //开启Spark性能监控功能
      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
//    SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext: SQLContext = new SQLContext(sc)
    //创建SparkStreamingContext对象
    val ssc = new StreamingContext(sc, Seconds(2))

    //将关键页面数据加载到广播变量
    val criticalPagesList: ArrayBuffer[String] = AnalyzeRuleDB.queryCriticalPages()
    @volatile var criticalPagesBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(criticalPagesList)

    //将黑名单数据加载到广播变量
    val blackIPList: ArrayBuffer[String] = AnalyzeRuleDB.getIpBlackList()
    @volatile var blackIPBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(blackIPList)

    //将流程数据加载到广播变量
    val flowCollocations: ArrayBuffer[FlowCollocation] = AnalyzeRuleDB.createFlow()
    @volatile var flowCollocationsBroadcast: Broadcast[ArrayBuffer[FlowCollocation]] = sc.broadcast(flowCollocations)






    //自定义维护Offset
    val inputStream: InputDStream[(String, String)] = createKafkaStream(ssc)
    //从inputStream中取出消息
    val dStream: DStream[String] = inputStream.map(_._2)
    //将消息转换为ProcessedData对象
    val processedDataDStream: DStream[ProcessedData] = QueryDataPackage.queryDataLoadAndPackage(dStream)

    //获取Redis连接对象
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    processedDataDStream.foreachRDD(processedDataRDD => {
      //编写业务代码,更新广播变量,进行指标统计,分值计算
      //更新广播变量
      var criticalPagesFlag: String = jedis.get("criticalPagesFlag")
      //先判断classifyRuleChangeFlag是否为空
      if (StringUtils.isBlank(criticalPagesFlag)){
        criticalPagesFlag = "true"
        //重新设置到Redis中
        jedis.set("criticalPagesFlag", criticalPagesFlag)
      }
      if (criticalPagesFlag.toBoolean) {
        criticalPagesBroadcast.unpersist()
        //将关键页面数据加载到广播变量
        val criticalPagesList: ArrayBuffer[String] = AnalyzeRuleDB.queryCriticalPages()
        criticalPagesBroadcast = sc.broadcast(criticalPagesList)

        criticalPagesFlag = "false"
        //重新设置到Redis中
        jedis.set("criticalPagesFlag", criticalPagesFlag)
      }

      //更新黑名单信息
      var blackIPChangeFlag: String = jedis.get("BlackIPChangeFlag")
      //先判断classifyRuleChangeFlag是否为空
      if (StringUtils.isBlank(blackIPChangeFlag)){
        blackIPChangeFlag = "true"
        //重新设置到Redis中
        jedis.set("BlackIPChangeFlag", blackIPChangeFlag)
      }
      if (blackIPChangeFlag.toBoolean) {
        blackIPBroadcast.unpersist()
        //将黑名单数据加载到广播变量
        val blackIPList: ArrayBuffer[String] = AnalyzeRuleDB.getIpBlackList()
        blackIPBroadcast = sc.broadcast(blackIPList)

        blackIPChangeFlag = "false"
        //重新设置到Redis中
        jedis.set("BlackIPChangeFlag", blackIPChangeFlag)
      }

      //更新流程的广播变量flowCollocationsBroadcast
      var flowCollocationChangeFlag: String = jedis.get("flowCollocationChangeFlag")
      //先判断classifyRuleChangeFlag是否为空
      if (StringUtils.isBlank(flowCollocationChangeFlag)){
        flowCollocationChangeFlag = "true"
        //重新设置到Redis中
        jedis.set("flowCollocationChangeFlag", flowCollocationChangeFlag)
      }
      if (flowCollocationChangeFlag.toBoolean) {
        flowCollocationsBroadcast.unpersist()
        //将黑名单数据加载到广播变量
        val flowCollocations: ArrayBuffer[FlowCollocation] = AnalyzeRuleDB.createFlow()
        flowCollocationsBroadcast = sc.broadcast(flowCollocations)

        flowCollocationChangeFlag = "false"
        //重新设置到Redis中
        jedis.set("flowCollocationChangeFlag", flowCollocationChangeFlag)
      }






      //开始根据各个指标维度进行计算
      //计算IP段的访问量
      val ipBlockCountMap: collection.Map[String, Int] = CoreRule.ipBlockCount(processedDataRDD)
      //计算IP的5分钟访问量
      val ipCountMap: collection.Map[String, Int] = CoreRule.ipCount(processedDataRDD)
      //计算IP5分钟对关键页面的访问量
      val ipCriticalPagesMap: collection.Map[String, Int] = CoreRule.ipCriticalPagesCount(processedDataRDD, criticalPagesBroadcast.value)
      //计算IP5分钟携带不同UA的个数
      val ipUAMap: collection.Map[String, Int] = CoreRule.ipUACount(processedDataRDD)
      //计算IP5分钟访问关键页面最小时间间隔
      val ipCriticalPagesMinTimeMap: collection.Map[String, Long] = CoreRule.ipCriticalPagesMinTimeCount(processedDataRDD, criticalPagesBroadcast.value)
      //计算IP5分钟访问关键页面最小时间间隔小于预设值的次数
      val ipCriticalPagesMinNumMap: collection.Map[(String, String), Int] = CoreRule.ipCriticalPagesMinNumCount(processedDataRDD, criticalPagesBroadcast.value)
      //计算IP5分钟查询不同航班的次数
      val ipCityCountMap: collection.Map[String, Int] = CoreRule.ipCityCount(processedDataRDD)
      //计算IP5分钟携带不同Cookie的数量
      val ipCookieCountMap: collection.Map[String, Int] = CoreRule.ipCookieCount(processedDataRDD, criticalPagesBroadcast.value)


      //万事俱备,开始计算
      val antiCalculateResultRDD: RDD[AntiCalculateResult] = RuleUtil.calculateAntiResult(
        processedDataRDD,
        ipBlockCountMap,
        ipCountMap,
        ipCriticalPagesMap,
        ipUAMap,
        ipCriticalPagesMinTimeMap,
        ipCriticalPagesMinNumMap,
        ipCityCountMap,
        ipCookieCountMap,
        flowCollocationsBroadcast.value
      )
//      antiCalculateResultRDD包含所有的数据,需要先将非黑名单数据过滤掉,只要黑名单数据
      val filterBlackRDD: RDD[AntiCalculateResult] = antiCalculateResultRDD.filter(antiCalculateResult => {
        //过滤flowsScore集合数据
        val results: Array[FlowScoreResult] = antiCalculateResult.flowsScore.filter(flowsScore => {
          //如果超出范围,return true
          flowsScore.isUpLimited
        })
        //       对过滤之后的集合 results 进行判断,看是否有数据,如果results为空,那么就是没有黑名单数据
        results.nonEmpty
      })
      //去重重复的黑名单IP数据
      val resultRDD: RDD[(String, FlowScoreResult)] = filterBlackRDD.map(line => {
        //返回(ip,得分数据),因为只能开启1个流程规则,所以取出第一个就行了
        (line.ip, line.flowsScore(0))
      }).reduceByKey((x, y) => y)

      //将数据恢复到Redis中
      BlackListToRedis.blackListDataToRedis(jedis, sc, sqlContext)

      //将数据存入Redis
      val rowRDD: RDD[Row] = resultRDD.map(line => {

        val ip: String = line._1
        val antiBlackRecordByFlow: FlowScoreResult = line._2
        val jedisCluster: JedisCluster = JedisConnectionUtil.getJedisCluster

        //CSANTI_ANTI_BLACK:192.168.80.81:asdfasdfdsgdfs
        val blackListKey = PropertiesUtil.getStringByKey("cluster.key.anti_black_list", "jedisConfig.properties") + s"${ip}:${antiBlackRecordByFlow.flowId}"
        //流程得分|流程ID|命中的规则|命中时间
        val blackListValue = s"${antiBlackRecordByFlow.flowScore}|${antiBlackRecordByFlow.flowStrategyCode}|${antiBlackRecordByFlow.hitRules.mkString(",")}|${antiBlackRecordByFlow.hitTime}"
        val ex = PropertiesUtil.getStringByKey("cluster.exptime.anti_black_list", "jedisConfig.properties").toInt
        jedisCluster.setex(blackListKey, ex, blackListValue)
        //定义存储hdfs的超时时间,方便数据恢复的时候使用
        val rowEx = new Date().getTime + (ex * 1000) + ""
        Row(rowEx, blackListKey, blackListValue)
      })

      //将黑名单数据备份到HDFS上
      BlackListToHDFS.saveAntiBlackList(rowRDD, sqlContext)

    })
    /*
    *存储规则计算结果 RDD（antiCalculateResults） 到 HDFS */
    dStream.foreachRDD(line =>{
      val date = new SimpleDateFormat("yyyy/MM/dd/HH").format(System.currentTimeMillis())
      val yyyyMMddHH = date.replace("/","").toInt
      val path: String = PropertiesUtil.getStringByKey("blackListPath","HDFSPathConfig.properties")+"itheima/"+yyyyMMddHH
      try{
        sc.textFile(path+"/part-00000")
          //将当前数据和历史数据合并
          .union(line)
          //合并为1个文件
          .repartition(1)
          .saveAsTextFile(path)
      }catch{
        case e: Exception =>
          //如果路径不存在,走当前代码
          line.repartition(1).saveAsTextFile(path)
      }



      SparkStreamingMonitor.queryMonitor(sc, line)
    })




//    inputStream.print()


    //将数据偏移量到zookeeper中
    inputStream.foreachRDD(rdd => {
      //保存偏移量
      saveOffsets(rdd)
    })
    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }










  /**
    * 消费Kafka数据,创建InputStream对象
    *
    * @param ssc
    * @return
    */
  def createKafkaStream(ssc: StreamingContext): InputDStream[(String, String)] = {
    //连接Kafka
    //封装Kafka参数信息
    var kafkaParams = Map[String, String]()
    //从kafkaConfig.properties配置文件中获取broker列表信息
    val brokerList: String = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")
    kafkaParams += ("metadata.broker.list" -> brokerList)

    //zookeeper主机地址
    val zkHosts: String = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    //创建zk客户端对象
    val zkClient = new ZkClient(zkHosts)
    //topic信息存储位置
    val zkPath: String = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")
    //topic
    val topic: String = PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties")
    //封装topic的集合
    val topics = Set[String](topic)


    //使用KafkaOffsetUtil来获取TopicAndPartition数据
    val topicAndPartitionOption: Option[Map[TopicAndPartition, Long]] = KafkaOffsetUtil.readOffsets(zkClient, zkHosts, zkPath, topic)

    val inputStream: InputDStream[(String, String)] = topicAndPartitionOption match {
      //如果有数据:从Zookeeper中读取偏移量
      case Some(topicAndPartition) =>
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, topicAndPartition, messageHandler)
      //如果没有数据,还按照以前的方式来读取数据
      case None => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    inputStream
  }

  /**
    * 保存偏移量信息
    * @param rdd
    */
  def saveOffsets(rdd: RDD[(String, String)]): Unit = {
    //zookeeper主机地址
    val zkHosts: String = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    //创建zk客户端对象
    val zkClient = new ZkClient(zkHosts)
    //topic信息存储位置
    val zkPath: String = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")

    KafkaOffsetUtil.saveOffsets(zkClient, zkHosts, zkPath, rdd)
  }
}

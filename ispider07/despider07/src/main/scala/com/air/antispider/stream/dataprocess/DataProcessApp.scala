package com.air.antispider.stream.dataprocess

import com.air.antispider.stream.common.bean.{AnalyzeRule, BookRequestData, ProcessedData, QueryRequestData, RequestType}
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.dataprocess.businessprocess.{AnalyzeBookRequest, AnalyzeRequest, AnalyzeRuleDB, BusinessProcess, DataPackage, DataSplit, EncryptedData, IpOperation, RequestTypeClassifier, SendData, SparkStreamingMonitor, TravelTypeClassifier, URLFilter}
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

/**
  * 数据预处理的主程序
  */
object DataProcessApp {

  def main(args: Array[String]): Unit = {
    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //创建Spark配置对象
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DataProcessApp")
      //开启Spark性能监控功能
      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    //创建SparkStreamingContext对象
    val ssc = new StreamingContext(sc, Seconds(2))

    //加载数据库规则,放入广播变量
    val filterRuleList: ArrayBuffer[String] = AnalyzeRuleDB.getFilterRule()
    //将过滤规则列表放入广播变量
    //@volatile 让多个线程能够安全的修改广播变量
    @volatile var filterRuleBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(filterRuleList)
    //将分类规则加载到广播变量
    val classifyRuleMap: Map[String, ArrayBuffer[String]] = AnalyzeRuleDB.getClassifyRule()
    @volatile var classifyRuleBroadcast: Broadcast[Map[String, ArrayBuffer[String]]] = sc.broadcast(classifyRuleMap)

    //加载解析规则信息到广播变量
    val queryRuleList: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(0)
    val bookRuleList: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(1)
    @volatile var queryRuleBroadcast: Broadcast[List[AnalyzeRule]] = sc.broadcast(queryRuleList)
    @volatile var bookRuleBroadcast: Broadcast[List[AnalyzeRule]] = sc.broadcast(bookRuleList)

    //将黑名单数据加载到广播变量
    val blackIPList: ArrayBuffer[String] = AnalyzeRuleDB.getIpBlackList()
    @volatile var blackIPBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(blackIPList)










    //消费Kafka消息,有几种方式?2种

    var kafkaParams = Map[String, String]()

    //从kafkaConfig.properties配置文件中获取broker列表信息
    val brokerList: String = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")
    kafkaParams += ("metadata.broker.list" -> brokerList)
    val topics = Set[String]("sz07")

    //使用Direct方式从Kafka中消费数据
    //StringDecoder:默认情况下,java的序列化性能不高,Kafka为了提高序列化性能,需要使用kafka自己的序列化机制
    val inputDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //获取的消息是(key,message)的形式,
    val messageDStream: DStream[String] = inputDStream.map(_._2)

    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    messageDStream.foreachRDD(messageRDD =>{
      //先检查数据库,更新广播变量
      var filterRuleChangeFlag = jedis.get("FilterRuleChangeFlag")
      //检查标记是否存在
      if (StringUtils.isBlank(filterRuleChangeFlag)) {
        filterRuleChangeFlag = "true"
        //重新设置到Redis中
        jedis.set("FilterRuleChangeFlag", filterRuleChangeFlag)
      }
      //更新广播变量
      if (filterRuleChangeFlag.toBoolean) {
        //先清除之前的广播变量
        filterRuleBroadcast.unpersist()
        //FilterRuleChangeFlag为true,代表需要重新更新广播变量
        //加载数据库规则,放入广播变量
        val filterRuleList: ArrayBuffer[String] = AnalyzeRuleDB.getFilterRule()
        //将过滤规则列表放入广播变量
        //@volatile 让多个线程能够安全的修改广播变量
        filterRuleBroadcast = sc.broadcast(filterRuleList)
        filterRuleChangeFlag = "false"
        jedis.set("FilterRuleChangeFlag", filterRuleChangeFlag)
      }

      //更新分类规则信息
      var classifyRuleChangeFlag: String = jedis.get("ClassifyRuleChangeFlag")
      //先判断classifyRuleChangeFlag是否为空
      if (StringUtils.isBlank(classifyRuleChangeFlag)){
        classifyRuleChangeFlag = "true"
        //重新设置到Redis中
        jedis.set("ClassifyRuleChangeFlag", classifyRuleChangeFlag)
      }
      if (classifyRuleChangeFlag.toBoolean) {
        classifyRuleBroadcast.unpersist()
        //将分类规则加载到广播变量
        val classifyRuleMap: Map[String, ArrayBuffer[String]] = AnalyzeRuleDB.getClassifyRule()
        classifyRuleBroadcast = sc.broadcast(classifyRuleMap)
        classifyRuleChangeFlag = "false"
        //重新设置到Redis中
        jedis.set("ClassifyRuleChangeFlag", classifyRuleChangeFlag)
      }

      //更新解析规则信息
      var analyzeRuleChangeFlag: String = jedis.get("AnalyzeRuleChangeFlag")
      //先判断classifyRuleChangeFlag是否为空
      if (StringUtils.isBlank(analyzeRuleChangeFlag)){
        analyzeRuleChangeFlag = "true"
        //重新设置到Redis中
        jedis.set("AnalyzeRuleChangeFlag", analyzeRuleChangeFlag)
      }
      if (analyzeRuleChangeFlag.toBoolean) {
        queryRuleBroadcast.unpersist()
        bookRuleBroadcast.unpersist()
        //将解析规则加载到广播变量
        //加载解析规则信息到广播变量
        val queryRuleList: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(0)
        val bookRuleList: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(1)
        queryRuleBroadcast = sc.broadcast(queryRuleList)
        bookRuleBroadcast = sc.broadcast(bookRuleList)

        analyzeRuleChangeFlag = "false"
        //重新设置到Redis中
        jedis.set("AnalyzeRuleChangeFlag", analyzeRuleChangeFlag)
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



      //开启链路统计功能
      val serversCountMap: collection.Map[String, Int] = BusinessProcess.linkCount(messageRDD)
      //URL过滤功能
      val filterRDD: RDD[String] = messageRDD.filter(message => URLFilter.filterURL(message, filterRuleBroadcast.value))

      //进行数据脱敏操作
      //加密手机号
      val encryptedPhoneRDD: RDD[String] = EncryptedData.encryptedPhone(filterRDD)
      //加密身份证号
      val encryptedRDD: RDD[String] = EncryptedData.encryptedID(encryptedPhoneRDD)

      //进行数据信息提取/转换等操作,得到ProcessedDataRDD
      val processedDataRDD: RDD[ProcessedData] = encryptedRDD.map(message => {
        //获取到消息后开始进行数据切割/打标签等操作
        //数据切割
        val (request, //请求URL
        requestMethod,
        contentType,
        requestBody, //请求体
        httpReferrer, //来源URL
        remoteAddr, //客户端IP
        httpUserAgent,
        timeIso8601,
        serverAddr,
        cookiesStr,
        cookieValue_JSESSIONID,
        cookieValue_USERID) = DataSplit.split(message)

        //对请求的分类进行打标签操作
        val requestType: RequestType = RequestTypeClassifier.requestTypeClassifier(request, classifyRuleBroadcast.value)
        //对往返数据进行打标签操作
        val travelType: TravelTypeEnum = TravelTypeClassifier.travelTypeClassifier(httpReferrer)
        //开始解析数据
        //解析查询数据
        val queryParams: Option[QueryRequestData] = AnalyzeRequest.analyzeQueryRequest(
          requestType,
          requestMethod,
          contentType,
          request,
          requestBody,
          travelType,
          queryRuleBroadcast.value)
        //解析预定数据
        val bookParams: Option[BookRequestData] = AnalyzeBookRequest.analyzeBookRequest(
          requestType,
          requestMethod,
          contentType,
          request,
          requestBody,
          travelType,
          bookRuleBroadcast.value
        )
        //数据加工操作
        val highFrqIPGroup: Boolean = IpOperation.operationIP(remoteAddr, blackIPBroadcast.value)
        //对上面的散乱数据进行封装
        val processedData: ProcessedData = DataPackage.dataPackage(
          "", //原始数据,此处直接置为空
          requestMethod,
          request,
          remoteAddr,
          httpUserAgent,
          timeIso8601,
          serverAddr,
          highFrqIPGroup,
          requestType,
          travelType,
          cookieValue_JSESSIONID,
          cookieValue_USERID,
          queryParams,
          bookParams,
          httpReferrer)
        processedData
      })

      //将结构化的数据ProcessedData根据不同的请求发送到不同的Topic中
      //发送查询数据到Kafka
      SendData.sendQueryDataKafka(processedDataRDD)
      //发送预定数据到Kafka
      SendData.sendBookDataKafka(processedDataRDD)

      //开启Spark性能监控
      //SparkContext, 数据集RDD, 数据采集结果信息
      SparkStreamingMonitor.streamMonitor(sc, processedDataRDD, serversCountMap)


      processedDataRDD.foreach(println)
    })

    //启动Spark程序
    ssc.start()
    ssc.awaitTermination()
  }



}

package com.air.antispider.stream.rulecompute.businessprocess

import java.util.Date

import com.air.antispider.stream.common.bean.{FlowCollocation, ProcessedData, RuleCollocation}
import com.air.antispider.stream.rulecompute.bean.{AntiCalculateResult, FlowScoreResult}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object RuleUtil {



  /**
    * 通过各个规则计算流程最终结果
    *
    * @param processedDataRDD
    * @param ipBlockCountMap
    * @param ipCountMap
    * @param ipCriticalPagesMap
    * @param ipUAMap
    * @param ipCriticalPagesMinTimeMap
    * @param ipCriticalPagesMinNumMap
    * @param ipCityCountMap
    * @param ipCookieCountMap
    * @param flowCollocationList
    */
  def calculateAntiResult(
                           processedDataRDD: RDD[ProcessedData],
                           ipBlockCountMap: collection.Map[String, Int],
                           ipCountMap: collection.Map[String, Int],
                           ipCriticalPagesMap: collection.Map[String, Int],
                           ipUAMap: collection.Map[String, Int],
                           ipCriticalPagesMinTimeMap: collection.Map[String, Long],
                           ipCriticalPagesMinNumMap: collection.Map[(String, String), Int],
                           ipCityCountMap: collection.Map[String, Int],
                           ipCookieCountMap: collection.Map[String, Int],
                           flowCollocationList: ArrayBuffer[FlowCollocation]
                         ): RDD[AntiCalculateResult] = {

    //从map中获取各个指标的数据
    val antiCalculateResultRDD: RDD[AntiCalculateResult] = processedDataRDD.map(processedData => {
      val ip: String = processedData.remoteAddr
      val url: String = processedData.request

      //获取IP的前2位, 192.168
      val arr: Array[String] = ip.split("\\.")
      var ipBlock = ""
      if (arr.length == 4) {
        //代表这是一个完整的IP
        ipBlock = arr(0) + "." + arr(1)
      }
      //获取IP段的值
      val ipBlockCounts: Int = ipBlockCountMap.getOrElse(ipBlock, 0)
      //获取IP的值
      val ipCounts: Int = ipCountMap.getOrElse(ip, 0)
      //获取关键页面的值
      val ipCriticalPagesCounts: Int = ipCriticalPagesMap.getOrElse(ip, 0)
      val ipUACounts: Int = ipUAMap.getOrElse(ip, 0)
      //最小访问时间间隔,如果获取不到IP,给个Int最大值,不能给0
      val ipCriticalPagesMinTimeCounts: Int = ipCriticalPagesMinTimeMap.getOrElse(ip, Integer.MAX_VALUE).toString.toInt
      val ipCriticalPagesMinNumCounts: Int = ipCriticalPagesMinNumMap.getOrElse((ip, url), 0)
      val ipCityCounts: Int = ipCityCountMap.getOrElse(ip, 0)
      val ipCookieCounts: Int = ipCookieCountMap.getOrElse(ip, 0)

      //定义map封装规则分值信息
      val map = Map[String, Int](
        "ipBlock" -> ipBlockCounts,
        "ip" -> ipCounts,
        "criticalPages" -> ipCriticalPagesCounts,
        "userAgent" -> ipUACounts,
        "criticalPagesAccTime" -> ipCriticalPagesMinTimeCounts,
        "criticalPagesLessThanDefault" -> ipCriticalPagesMinNumCounts,
        "flightQuery" -> ipCityCounts,
        "criticalCookies" -> ipCookieCounts
      )


      val flowsScore: Array[FlowScoreResult] = computeFlowScore(map, flowCollocationList)


      AntiCalculateResult(
        processedData,
        ip,
        ipBlockCounts,
        ipCounts,
        ipCriticalPagesCounts,
        ipUACounts,
        ipCriticalPagesMinTimeCounts,
        ipCriticalPagesMinNumCounts,
        ipCityCounts,
        ipCookieCounts,
        flowsScore
      )
    })
    antiCalculateResultRDD
  }

  /**
    * 开始计算,获取最终计算结果
    * @param map
    * @param flowCollocationList
    * @return
    */
  def computeFlowScore(map: Map[String, Int], flowCollocationList: ArrayBuffer[FlowCollocation]): Array[FlowScoreResult] = {
    //定义集合存储每一个流程的计算结果
    var flowScoreResultList = new ArrayBuffer[FlowScoreResult]()
    //因为传过来的flowCollocationList代表多个流程,所以先循环流程
    for (flow <- flowCollocationList) {
      //通过flow,获取该流程下的规则
      val rules: List[RuleCollocation] = flow.rules

      //定义集合存储超出范围的规则得分信息
      var array1 = new ArrayBuffer[Double]()
      //定义超出范围,并且处于开启状态的得分信息
      var array2 = new ArrayBuffer[Double]()
      //定义命中记录的集合,并且处于开启状态的得分信息
      var hitRules = new ArrayBuffer[String]()

      for (rule <- rules) {
        val ruleName: String = rule.ruleName
        //从Map中取出计算结果
        val num: Int = map.getOrElse(ruleName, 0)
        //取出第一个值
        var limit = rule.ruleValue0
        if ("criticalPagesLessThanDefault".equals(ruleName)){
          //如果当前的指标为criticalPagesLessThanDefault,那么取出第二个值
          limit = rule.ruleValue1
        }
        //如果数据库名称和计算结果名称一样,开始比较大小
        if (num > limit) {
          //如果当前计算结果超出了数据库配置好的阈值范围,那么就命中该规则
          //将得分放入集合
          array1 += rule.ruleScore
          if (rule.ruleStatus == 0){
            //如果当前规则状态为开启状态
            array2 += rule.ruleScore
            hitRules += ruleName
          }
        }
      }

      //调用算法,开始计算最终得分
      val flowScore = calculateFlowScore(array1.toArray, array2.toArray)
      //是否超出阈值范围
      val isUpLimited = flowScore > flow.flowLimitScore

      //命中时间
      val hitTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(new Date)


      //封装计算结果对象
      flowScoreResultList += FlowScoreResult(flow.flowId,
        flowScore,
        flow.flowLimitScore,
        isUpLimited,
        flow.flowId,  //流程ID
        hitRules.toList, //命中了哪些规则
        hitTime) //命中的时间
    }
    flowScoreResultList.toArray
  }
  /**
    * 计算流程得分-请参考详细设计说明书（规则打分，流程计算）及对应的原型设计（流
程管理）
    * 系数 2 权重：60%，数据区间：10-60
    * 系数 3 权重：40，数据区间：0-40
    * 系数 2+系数 3 区间为：10-100
    * 系数 1 为:平均分/10
    * 所以，factor1 * (factor2 + factor3)区间为:平均分--10 倍平均分
    * @param scores result 二维数组
    * @param xa isTriggered 数组
    * @return 规则得分
    */
  def calculateFlowScore(scores: Array[Double], xa: Array[Double]): Double = {
    //总打分
    val sum = scores.sum
    //打分列表长度
    val dim = scores.length
    if (dim == 0) {
      return 0
    }
    //系数 1：平均分/10
    val factor1 = sum / (10 * dim)

    //命中规则中，规则分数最高的
    val maxInXa = if (xa.isEmpty) {
      0.0
    } else {
      xa.max
    }
    //系数 2：系数 2 的权重是 60，指的是最高 score 以 6 为分界，最高 score 大于 6，就给满权重 60，不足 6，就给对应的 maxInXa*10
    val factor2 = if (1 < (1.0 / 6.0) * maxInXa) {
      60
    } else {
      (1.0 / 6.0) * maxInXa * 60
    }
    //系数 3：打开的规则总分占总规则总分的百分比，并且系数 3 的权重是 40
    val factor3 = 40 * (xa.sum / sum)
    /**
      * 系数 2 权重：60%，数据区间：10-60
      * 系数 3 权重：40，数据区间：0-40
      * 系数 2+系数 3 区间为：10-100
      * 系数 1 为:平均分/10
      * 所以，factor1 * (factor2 + factor3)区间为:平均分--10 倍平均分
      */
    factor1 * (factor2 + factor3)
  }
}

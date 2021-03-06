package com.csair.b2c.gciantispider.offlinecalculation.statisticaloffline

/**
  * purpose:
  * version:
  * date:
  * author:wangsf.
  */

import java.sql.Date
import java.util.UUID

import com.csair.b2c.gciantispider.offlinecalculation.util.SparkMySqlProperties
import com.csair.b2c.gciantispider.util.PropertiesUtil
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算可视化数据
  * 数据来源是streaming存储到sparksql中的数据
  *
  */
case class Requests(requestMethod: String,
                    request: String,
                    remoteAddr: String,
                    httpUserAgent: String,
                    timeIso8601: String,
                    serverAddr: String,
                    criticalCookie: String,
                    highFrqIPGroup: String, //是否是高频IP
                    flightType: String, //国内/国际
                    behaviorType: String, //查询/预定
                    travelType: String, //单程/往返
                    flightDate: String, //飞行日期
                    depcity: String, //出发地
                    arrcity: String, //目的地
                    JSESSIONID: String,
                    USERID: String,
                    queryRequestDataStr: String, //查询请求参数
                    bookRequestDataStr: String, //预定请求参数
                    httpReferrer: String,
                    StageTag: String, //目前用户执行的哪个阶段:查询/预定/支付...
                    Spider: Int)

case class BlackList(
                      remoteAddr: String,
                      FlowID: String,
                      Score: String,
                      StrategicID: String)

object VisualizationIndicators {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf = new SparkConf().setMaster("local").setAppName("visualization").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //隐式类型转换
    import sqlContext.implicits._
    /**
      * 读取原始数据，注册为表
      */
    //数据路径
    val defaultPathConfig = "offlineConfig.properties"
    //定义一个HDFS的路径,项目最终从这个路径下获取数据
    val filePath = PropertiesUtil.getStringByKey("inputFilePath", defaultPathConfig)
    //从本地文件获取数据
    //读取kafka中的原始日志信息
    val request: DataFrame = sc.textFile("C:\\Users\\75826\\Desktop\\反扒参考资料\\数据样本\\test\\part-00000")
      //将一行字符串转换为一个数组
      .map(_.split("#CS#"))
      //将一个数组转换为对象
      .map(p => Requests(p(1), p(2), p(3), p(4), p(5), p(6), "", p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), "", 0))
      .toDF()
    //将当前获取到的数据注册为表,表名为"request"
    request.registerTempTable("request")
    //读取黑名单信息
    val SpiderIP = sc.textFile("C:\\Users\\75826\\Desktop\\反扒参考资料\\数据样本\\SpiderIP.txt")
      .map(_.split("\\|"))
      .map(p => BlackList(p(0), p(1), p(2), p(3)))
      .toDF()
    //注册为表SpiderIP
    SpiderIP.registerTempTable("SpiderIP")
    /**
      * 标记爬虫
      */
    val AddSpiderTag = sqlContext.sql("select * from request left outer join SpiderIP where request.remoteAddr = SpiderIP.remoteAddr")

    //查询转化率必须的字段
    val TransformRateNeeded = AddSpiderTag.select(
      AddSpiderTag("request"),
      AddSpiderTag("JSESSIONID"),
      AddSpiderTag("StageTag"),
      AddSpiderTag("FlowID"),
      AddSpiderTag("travelType"),
      AddSpiderTag("flightType"))
    //注册为TransformRateNeeded表,里面包含转换率等指标相关的字段
    TransformRateNeeded.registerTempTable("TransformRateNeeded")
    //打印表信息
//    TransformRateNeeded.show()
    //2019-01-01T12:01:01+8:00
    val date: String = AddSpiderTag.select(AddSpiderTag("timeIso8601")).first().get(0).toString.split("T")(0)
    val dataTime: Date = Date.valueOf(date)

    /**
      * 打阶段标签
      * regester(UDF函数的名字, 执行的操作)
      */

    sqlContext.udf.register("Stage", (request: String) => {
      if (request.matches("^.*/bookingnew/.*$") //预定、商城
        || request.matches("^.*/bookingGroup/.*$")
        || request.matches("^.*/ita/intl/zh/shop/.*$")) {
        "1"
      } else if (request.matches("^.*/modules/permissionnew/.*$") //乘客信息
        || request.matches("^.*/ita/intl/zh/passengers/.*$")) {
        "2"
      } else if (request.matches("^.*upp_payment/pay/.*$")) {
        //支付
        "3"
      } else {
        //其他，查询等操作
        "0"
      }
    })
    //FlowID不为空时对应为爬虫
    sqlContext.udf.register("Spider", (FlowID: String) => {
      if (FlowID == null || "null".equalsIgnoreCase(FlowID)) {
        //非爬虫
        "0"
      } else {
        //爬虫
        "1"
      }
    })
    /**
      * 过滤数据
      */
    //原始数据：request,JSESSIONID,Stage(request) as StageTag, Spider(FlowID) as SpiderTag, flightType, travelType
    val request1 = sqlContext.sql("select request,JSESSIONID,Stage(request) as StageTag, Spider(FlowID) as SpiderTag, flightType, travelType from TransformRateNeeded")
    //从表TransformRateNeeded中过滤掉JSESSIONID为空的数据
    val request1_transformed = request1.filter(!request1("JSESSIONID").contains("NULL"))

//    request1_transformed.show(1000)



    /*
    国内查询转化率
    1、在request1_transformed表中过滤出是国内的操作
    2、在上面数据的基础上过滤出StageTag=2/在上面数据的基础上过滤出StageTag=1
*/
 /*   val NatinalRate_1 =
      request1_transformed
        .filter(request1_transformed("flightType").equalTo("National"))
        .filter(request1_transformed("StageTag").equalTo("2"))
        .count().toFloat /
        request1
          .filter(request1("flightType").equalTo("National"))
          .filter(request1("StageTag").equalTo("1"))
          .count().toFloat
    //将计算结果NatinalRate_1存入MySQL数据库中
    //通过并行化创建 RDD
    val nh_domestic_inter_conversion_rate_RDD =
      sc.parallelize(Array(UUID.randomUUID().toString() + "," + "0," + "0," + NatinalRate_1)).map(_.split(","))
    //通过 StructType 直接指定每个字段的 schema
    val schema = StructType(List(StructField("id", StringType, true), StructField("step_type", IntegerType, true), StructField("flight_type", IntegerType, true), StructField("conversion_rate", FloatType, true), StructField("record_time", DateType, true)))
    //将 RDD 映射到 rowRDD
    val rowRDD = nh_domestic_inter_conversion_rate_RDD.map(p => Row(p(0), p(1).toInt, p(2).toInt, p(3).toFloat, dataTime))
    //将 schema 信息应用到 rowRDD 上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://192.168.80.81:3306/gciantispider", "gciantispider.nh_domestic_inter_conversion_rate", SparkMySqlProperties.getProperty())
    println("国内查询转化率")
    println(NatinalRate_1)*/

   /* 国际查询转化率
    1、在request1_transformed表中过滤出是国际的操作
    2、在上面数据的基础上过滤出StageTag=2/在上面数据的基础上过滤出StageTag=1
*/
  /*  val InternatinalRate_1 =
      request1_transformed
        .filter(request1_transformed("flightType").equalTo("Internatinal"))
        .filter(request1_transformed("StageTag").equalTo("2"))
        .count().toFloat /
        request1
          .filter(request1("flightType").equalTo("Internatinal"))
          .filter(request1("StageTag").equalTo("1"))
          .count().toFloat
    //通过并行化创建 RDD
    val nh_domestic_inter_conversion_rate_RDD2 =
      sc.parallelize(Array(UUID.randomUUID().toString() + "," + "0," + "1," + InternatinalRate_1)).map(_.split(","))
    //通过 StructType 直接指定每个字段的 schema
    val schema = StructType(List(StructField("id", StringType, true), StructField("step_type", IntegerType, true), StructField("flight_type", IntegerType, true), StructField("conversion_rate", FloatType, true), StructField("record_time", DateType, true)))
    //通过 StructType 直接指定每个字段的 schema
    val rowRDD2 = nh_domestic_inter_conversion_rate_RDD2.map(p => Row(p(0), p(1).toInt, p(2).toInt, p(3).toFloat, dataTime))
    //将 schema 信息应用到 rowRDD 上
    val personDataFrame2 = sqlContext.createDataFrame(rowRDD2, schema)*/
    //将数据追加到数据库
    //personDataFrame2.write.mode("append").jdbc("jdbc:mysql://192.168.56.204:3306/gciantispider", "gciantispider.nh_domestic_inter_conversion_rate", SparkMySqlProperties.getProperty())
    //println("国际查询转化率")
    //println(InternatinalRate_1)

  /*
   国内航班选择-旅客信息 转化率
   1、在request1_transformed表中过滤出是National的操作
   2、StageTag=3/StageTag=2
    */
    val NatinalRate_2 = request1_transformed
      .filter(request1_transformed("flightType").equalTo("National"))
      .filter(request1_transformed("StageTag").equalTo("3"))
      .count().toFloat /
      request1
        .filter(request1("flightType").equalTo("National"))
        .filter(request1("StageTag").equalTo("2"))
        .count().toFloat
    println("国内航班选择-旅客信息 转化率")
    println(NatinalRate_2)
    /*
      国际航班选择-旅客信息 转化率
   1、在request1_transformed表中过滤出是Internatinal的操作
   2、StageTag=3/StageTag=2
    */
    val InternatinalRate_2 = request1_transformed.filter(request1_transformed("flightType").equalTo("Internatinal")).filter(request1_transformed("StageTag").equalTo("3")).count().toFloat /
      request1.filter(request1("flightType").equalTo("Internatinal")).filter(request1("StageTag").equalTo("2")).count().toFloat
    println("国际航班选择-旅客信息 转化率")
    println(InternatinalRate_2)


    /*
    爬虫用户转化率
     */
    val SpiderUserRate_1 = request1_transformed
      .filter(request1_transformed("SpiderTag").equalTo("1"))
      .filter(request1_transformed("StageTag").equalTo("2"))
      .count().toFloat /
      request1.filter(request1("SpiderTag").equalTo("1"))
        .filter(request1("StageTag").equalTo("1"))
        .count().toFloat
    println("爬虫用户 转化率")
    println(SpiderUserRate_1)
 /*
    正常用户转化率
     */
    val NormalUserRate_1 = request1_transformed
      .filter(request1_transformed("SpiderTag").equalTo("0"))
      .filter(request1_transformed("StageTag").equalTo("2"))
      .count().toFloat /
      request1.filter(request1("SpiderTag").equalTo("0"))
        .filter(request1("StageTag").equalTo("1"))
        .count().toFloat
    println("正常用户 转化率")
    println(NormalUserRate_1)
       /*
      全部用户转化率
      */
    val TotalUserRate_1 = request1_transformed
      .filter(request1_transformed("StageTag").equalTo("2"))
      .count().toFloat /
      request1
        .filter(request1("StageTag").equalTo("1")).count().toFloat
    println("全部用户 转化率")
    println(TotalUserRate_1)

  /*
    爬虫用户 旅客信息转化率
     */
    val SpiderUserRate_2 = request1_transformed.filter(request1_transformed("SpiderTag").equalTo("1")).filter(request1_transformed("StageTag").equalTo("3")).count().toFloat /
      request1.filter(request1("SpiderTag").equalTo("1")).filter(request1("StageTag").equalTo("2")).count().toFloat
    println("爬虫用户 航班选择-旅客信息转化率")
    println(SpiderUserRate_2)
    /*
    正常用户 旅客信息转化率
     */
    val NormalUserRate_2 = request1_transformed.filter(request1_transformed("SpiderTag").equalTo("0")).filter(request1_transformed("StageTag").equalTo("3")).count().toFloat /
      request1.filter(request1("SpiderTag").equalTo("0")).filter(request1("StageTag").equalTo("2")).count().toFloat
    println("正常用户 航班选择-旅客信息转化率")
    println(NormalUserRate_2)
    /*
    全部用户 旅客信息转化率
     */
    val TotalUserRate_2 = request1_transformed.filter(request1_transformed("StageTag").equalTo("3")).count().toFloat /
      request1.filter(request1("StageTag").equalTo("2")).count().toFloat
    println("全部用户 航班选择-旅客信息转化率")
    println(TotalUserRate_2)

    /*
      国内单程、国内双程、国际单程、国际双程查询爬取频次
      1、针对每天的数据进行爬虫的过滤
      2、国内单程查询、国际单程查询、国内双程查询、国际双程查询：	先根据（flightType、travelType）分组，然后进行count操作
  */
    val SpiderRuleByTravel = request1_transformed
      .filter(request1_transformed("SpiderTag").equalTo("1"))
      .groupBy(request1_transformed("flightType"), request1_transformed("travelType")).count()
    println("国内单程、国内双程、国际单程、国际双程查询爬取频次")
    SpiderRuleByTravel.show()

    /*
    爬虫用户查定比
     */
    val SpiderQueryBookRatio = request1_transformed
      .filter(request1_transformed("SpiderTag").equalTo("1"))
      .filter(request1_transformed("StageTag").equalTo("1")) //预定
      .count().toFloat /
      request1.filter(request1("SpiderTag").equalTo("1"))
        .filter(request1("StageTag").equalTo("0")) //查询
        .count().toFloat
    println("爬虫用户查定比")
    println(SpiderQueryBookRatio)
    /*
  正常用户查定比
   */
    val NormalQueryBookRatio = request1.filter(request1("SpiderTag").equalTo("0")).filter(request1("StageTag").equalTo("1")).count().toFloat /
      request1.filter(request1("SpiderTag").equalTo("0")).filter(request1("StageTag").equalTo("0")).count().toFloat
    println("正常用户查定比")
    println(NormalQueryBookRatio)
    /**
      * 爬虫对系统稳定性的影响
      */
    /*
    爬虫流量情况
     */
    val SpiderFlowVolumn = request1.filter(request1("SpiderTag").equalTo("1")).count()
    println("爬虫流量情况")
    println(SpiderFlowVolumn)
    /*
   正常流量情况
    */
    val NormalFlowVolumn = request1.filter(request1("SpiderTag").equalTo("0")).count()
    println("正常流量情况")
    println(NormalFlowVolumn)
    /*
    全部流量情况
    */
    val allFlowVolumn = request1.count()
    println("正常流量情况")
    println(allFlowVolumn)
  }


}

package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum.BehaviorTypeEnum
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

object SendData {
  /**
    * 发送预定数据到Kafka
    *
    * @param processedDataRDD
    */
  def sendBookDataKafka(processedDataRDD: RDD[ProcessedData]) = {
    sendToKafka(processedDataRDD, 1)
  }

  /**
    * 发送查询数据到Kafka
    *
    * @param processedDataRDD
    */
  def sendQueryDataKafka(processedDataRDD: RDD[ProcessedData]) = {
    sendToKafka(processedDataRDD, 0)
  }

  /**
    * 根据指定的类型,发送到Kafka
    *
    * @param processedDataRDD
    * @param topicType 0: 查询,1: 预定
    */
  def sendToKafka(processedDataRDD: RDD[ProcessedData], topicType: Int) = {
    //将processedData数据发送到Kafka中
    val messageRDD: RDD[String] = processedDataRDD
      //根据类型进行过滤
      .filter(processedData => processedData.requestType.behaviorType.id == topicType)
      //将数据转换为字符串
      .map(processedData => processedData.toKafkaString())

    //如果经过过滤操作后,还有数据,那么就发送
    if (!messageRDD.isEmpty()) {
      //定义Kafka相关配置
      //查询数据的 topic：target.query.topic = processedQuery
      var topicKey = ""
      if (topicType == 0) {
        topicKey = "target.query.topic"
      } else if (topicType == 1) {
        topicKey = "target.book.topic"
      }
      val queryTopic = PropertiesUtil.getStringByKey(topicKey, "kafkaConfig.properties")
      //创建 map 封装 kafka 参数
      val props = new java.util.HashMap[String, Object]()
      //设置 brokers
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
      //key 序列化方法
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
      //value 序列化方法
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))
      //批发送设置：32KB 作为一批次或 10ms 作为一批次
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
      props.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))

      messageRDD.foreachPartition(iter => {
        //先创建Kafka连接
        val producer = new KafkaProducer[String, String](props)
        //发送数据
        iter.foreach(message => {
          //发送数据
          producer.send(new ProducerRecord[String, String](queryTopic, message))
        })
        //关闭Kafka连接
        producer.close()
      })
    }
  }

}

package com.air.antispider.stream.common.bean

/**
 * 流程类：对应表nh_process_info
 * （规则配置集合和阈值参数）
 */
case class FlowCollocation(
                            flowId: String,  //流程ID
                            flowName: String,  //流程名称
                            rules: List[RuleCollocation], //该流程下的规则
                            flowLimitScore: Double = 100, //该流程的策略分值(阈值)
                            strategyCode:String  //流程ID(预留字段)
                          )

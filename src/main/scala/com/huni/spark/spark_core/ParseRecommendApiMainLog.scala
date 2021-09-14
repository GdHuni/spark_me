package com.huni.spark.spark_core

import com.alibaba.fastjson._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ParseRecommendApiMainLog {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RecommendApiMainLog")
    val sc = new SparkContext(conf)

    val textFile: RDD[String] = sc.textFile("D:\\IdeaProjects\\spark_me\\src\\main\\resources\\bd-service-recommend-api-main-v0324-183312-578d7744b4-p6c4q-info.log.2021-08-29.192.168.212_bd-log-agent-9v4pm.tar.gz")

   // textFile.filter(x=>x.contains("coldBootPolicy@policy")).foreach(println(_))

    textFile.filter(x=>x.contains("coldBootPolicy@policy"))
      .map(x => {
        x.replace(" coldBootPolicy@policy","\u0001coldBootPolicy@policy")
          .replace("^ ","\u0001")
          .replace("^","\u0001")
    }).saveAsTextFile("D:\\Interface_Log.1")

  }



  // 暴力破解是否为json
  def isJSONValid(test: String): Boolean = {
    try {
      JSON.parseObject(test)
    }catch {
      case ex: JSONException => {
          try JSON.parseArray(test)
          catch {
            case ex1: JSONException =>
              return false
          }
        }
    }
    true
  }
}

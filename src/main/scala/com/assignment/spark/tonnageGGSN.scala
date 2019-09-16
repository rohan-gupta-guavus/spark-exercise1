package com.assignment.spark

import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
import com.assignment.spark.bo.ggsnConst._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.commons.lang3.StringUtils
import com.assignment.spark.Utils.ConfigReader._
import com.assignment.spark.bo.TonnageConst._


object tonnageGGSN {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().appName("tonnageGGSN").getOrCreate()
    logger.info("Application ID -> " + spark.sparkContext.applicationId)
    //val configFilePath = args(0)
    //val inputConfig = readConfig(configFilePath)
    //val inputDatabase = inputConfig.getString("InputDatabase")
    //val inputTable = inputConfig.getString("InputTable")
    val inputDf = spark.sql("select * from rohan_test.edr_data_hive")
    val df = spark.read.option("rowTag","ggsn").option("attributePrefix", "c").option("valueTag", "valTag").xml("/opt/data/rohan/ggsn.xml")
    val ggsnDF = getggsnDf(df)
    val joinedDf = getJoinedDf(inputDf,ggsnDF)
    val finalDf = getAggregatedDf(joinedDf)
    finalDf.write.format("orc").saveAsTable("rohan_output.tonnageGGSN_table")
  }

  def getAggregatedDf(df:DataFrame) : DataFrame ={
    df.groupBy(getggsnName).
      agg(sum(col(getUplink)).as(getSumUplink),sum(col(getDownLink)).as(getSumDownlink),count(getReplyCodeCol).as(getHitsCol)).
      drop(getUplink,getDownLink).
      withColumn(getSumTotalBytes,col(getSumUplink)+col(getSumDownlink))
  }

  def getJoinedDf( inputDf : DataFrame , ggsnDF : DataFrame) : DataFrame = {
    inputDf.join(broadcast(ggsnDF),inputDf("ggsn_ip").equalTo(col(getggsnIP)),"left").
      drop(getggsnIP,"ggsn_ip").filter(col(getggsnName).isNotNull)
  }

  def getggsnDf(df:DataFrame) : DataFrame ={
    val tempDf = df.select(col(getInitaialGGsnCol).as(getggsnName), explode(col(getRuleColTable)).as(getRuleDfCol)).
      select(col(getggsnName), col("rule_col.condition.cvalue").as(getggsnIP))
    tempDf
  }
}

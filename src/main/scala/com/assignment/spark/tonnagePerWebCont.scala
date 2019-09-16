package com.assignment.spark

import com.assignment.spark.bo.contentMapper._
import com.typesafe.config.{Config, ConfigValueFactory}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import com.assignment.spark.bo.domainConst._
import com.assignment.spark.bo.TonnageConst._


object tonnagePerWebCont {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().appName("tonnagePerWebCont").getOrCreate()
    import spark.implicits._
    logger.info("Application ID -> " + spark.sparkContext.applicationId)
    val inputDf = spark.sql("select * from rohan_test.edr_data_hive")
    val contentDf = getContentmap.toSeq.toDF(getContent,getTypes)
    val mappedDf = mapContentType(inputDf,contentDf)
    val filteredDf = getFilteredDf(mappedDf)
    val udf_func : String => String = _.split("/")(2)
    val domain = udf(udf_func)
    val temp_df = filteredDf.withColumn(getDomainName,domain(col(gethttpUrl)))
    val AggregatedDf = getAggregatedDf(spark,temp_df)
    //val fillSeq = Seq(getDomainName,getTypes)
   // val finalDf = fillNa(AggregatedDf,fillSeq)
    val domainDf = AggregatedDf.filter(col(getTypes).isNull).withColumnRenamed(getDomainName,"domain_name_1").
     withColumnRenamed("hour","hour_1").
     withColumnRenamed("minute","minute_1").
     withColumnRenamed(getTypes,"Types_1").
     withColumnRenamed(getTotal,getSumTotal)
    val intermediateDf = AggregatedDf.join(domainDf,AggregatedDf(getDomainName).equalTo(domainDf("domain_name_1")) and AggregatedDf("hour").equalTo(domainDf("hour_1")) and AggregatedDf("minute").equalTo(domainDf("minute_1")),"left").
      drop("domain_name_1","hour_1","minute_1","Types_1","SumUplink","SumDownlink")
    logger.info("Joining of Aggregated Df with filtered df with Null values")
    val finalDf = intermediateDf.filter(col(getTypes).isNotNull).withColumn(getPercentCont,bround(col(getTotal)/col(getSumTotal)*100,3))
    finalDf.write.partitionBy("hour","minute").format("orc").saveAsTable("rohan_output.tonnagePerWebsiteCont")
  }

  def mapContentType (inputdf : DataFrame,contentDf : DataFrame) : DataFrame = {

    inputdf.join(broadcast(contentDf),inputdf(getContentType).contains(col(getContent)),"left").
      drop(getContentType,getContent).filter(col(getTypes).isNotNull)
  }

  def getFilteredDf (dataFrame: DataFrame) : DataFrame = {
    dataFrame.filter(col(gethttpUrl).rlike("^http://(?!/)..*$|^https://(?!/)..*$"))
  }

  def getAggregatedDf(spark : SparkSession,df : DataFrame) : DataFrame = {
    df.createOrReplaceTempView("TempView")
    val aggregations = "domain_name,Types,hour,minute, sum(transaction_uplink_bytes) as SumUplink , sum(transaction_downlink_bytes) as SumDownlink, ( sum(transaction_uplink_bytes) + sum(transaction_downlink_bytes) ) as Total"
    val groupByCols = "domain_name,Types,hour,minute"
    val groupingSets = "((domain_name,Types,hour,minute),(domain_name,hour,minute))"
    val aggDf = spark.sql("select "+ aggregations +" from TempView group by "+ groupByCols +" grouping sets "+ groupingSets)
    aggDf
  }

  def fillNa(df: DataFrame,fillSeq : Seq[String]): DataFrame ={
    val tempDf = df.withColumn(getDomainName,col(getDomainName).cast("string")).withColumn(getTypes,col(getTypes).cast("string")).na.fill("ALL",fillSeq)
    tempDf
  }
}

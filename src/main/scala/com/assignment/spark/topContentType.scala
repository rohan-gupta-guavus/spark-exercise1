package com.assignment.spark

import com.assignment.spark.bo.contentMapper._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.assignment.spark.bo.TonnageConst._
import com.assignment.spark.bo.domainConst._
import com.assignment.spark.bo.ggsnConst._

object topContentType {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().appName("tonnageContentType").getOrCreate()
    import spark.implicits._
    logger.info("Application ID -> " + spark.sparkContext.applicationId)
    val inputDf = spark.sql("select * from rohan_test.edr_data_hive")
    val contentDf = getContentmap.toSeq.toDF(getContent,getTypes)
    val mappedDf = mapContentType(inputDf,contentDf)
    val filteredDf = getFilteredDf(mappedDf)
    val AggregatedDf = getAggregatedDf(filteredDf)
    val finalDf = findTopN(AggregatedDf,5)

    finalDf.write.partitionBy("hour").mode("append").format("orc").
      saveAsTable("rohan_output.topNContent_Types")

  }

  def mapContentType (inputdf : DataFrame,contentDf : DataFrame) : DataFrame = {

    inputdf.join(broadcast(contentDf),inputdf(getContentType).contains(col(getContent)),"left").
      drop(getContentType,getContent).filter(col(getTypes).isNotNull)
  }

  def getFilteredDf (dataFrame: DataFrame) : DataFrame = {
    dataFrame.filter(col(getReplyCodeCol).rlike("^2[0-9][0-9]$"))
  }

  def getAggregatedDf (df : DataFrame) : DataFrame ={
    df.groupBy(getRadiusUserName,getTypes,"hour").
      agg(sum(col(getUplink)).as(getSumUplink),sum(col(getDownLink)).as(getSumDownlink),count(getReplyCodeCol).as(getHitsCol)).
      drop(getUplink,getDownLink)
  }

  def findTopN(df : DataFrame,n : Int) : DataFrame = {
    val partitionedWindow = Window.partitionBy(col(getRadiusUserName),col("hour"))
    val overUplink = partitionedWindow.orderBy(col(getSumUplink).desc)
    val overDownlink = partitionedWindow.orderBy(col(getSumDownlink).desc)
    val overHits = partitionedWindow.orderBy(col(getHitsCol).desc)

    val tempDf = df.withColumn(getRankUplink,dense_rank().over(overUplink)).withColumn(getRankDownlink,dense_rank().over(overDownlink)).withColumn(getRankHits,dense_rank().over(overHits))
    tempDf.filter((col(getRankUplink) <= n) or (col(getRankDownlink) <= n) or (col(getRankHits) <= n))
  }
}

package com.assignment.spark

import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.expressions.Window

object topTen {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().enableHiveSupport().appName("topTen").getOrCreate()
    logger.info("Application ID -> " + spark.sparkContext.applicationId)
    val inputDf = spark.sql("select radius_user_name,transaction_uplink_bytes,transaction_downlink_bytes,hour,minute from rohan_test.edr_data_hive").repartition(col("hour"),col("minute"))
    val aggregatedDf = performAggregations(inputDf)
    val finalDf = findTopTens(aggregatedDf)
    finalDf.write.partitionBy("hour").mode("append").format("orc").saveAsTable("rohan_output.topTens_table")
  }

  def performAggregations (dataFrame: DataFrame):DataFrame = {
    val tempDf = dataFrame.groupBy("radius_user_name","hour").agg(sum(col("transaction_uplink_bytes")).as("SumUplinkBytes"),sum(col("transaction_downlink_bytes")).as("SumDownlinkBytes")).drop("transaction_uplink_bytes","transaction_downlink_bytes")
    tempDf.withColumn("TotalBytes",col("SumUplinkBytes")+col("SumDownlinkBytes"))

  }

  def findTopTens(df: DataFrame) : DataFrame = {
    val partitionedWindow = Window.partitionBy(col("hour"))
    val overUplink = partitionedWindow.orderBy(col("SumUplinkBytes").desc)
    val overDownlink = partitionedWindow.orderBy(col("SumDownlinkBytes").desc)
    val overTotal = partitionedWindow.orderBy(col("TotalBytes").desc)

    val tempDf = df.withColumn("rank_uplink",dense_rank().over(overUplink)).withColumn("rank_downlink",dense_rank().over(overDownlink)).withColumn("rank_totalbytes",dense_rank().over(overTotal))
    tempDf.filter((col("rank_uplink") <= 10) or (col("rank_downlink") <= 10) or (col("rank_totalbytes") <= 10))
  }
}

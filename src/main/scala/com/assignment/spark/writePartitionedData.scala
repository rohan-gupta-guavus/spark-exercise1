package com.assignment.spark

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, _}

object writePartitionedData {
private val logger = LoggerFactory.getLogger(getClass)
private val filePath = ""
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("writePartitionedData").getOrCreate()
    logger.info("Application ID -> " + spark.sparkContext.applicationId)
    val rawDataDf = spark.read.option("header","true").option("inferSchema","true").csv("")
    val hour : String => Int = LocalDateTime.parse(_,DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")).getHour
    val minute : String => Int = LocalDateTime.parse(_,DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")).getMinute
    val hourUDF = udf(hour)
    val minuteUDF = udf(minute)

    val dataWithHourMinute = rawDataDf.withColumn("hour",hourUDF(col("sn-start-time"))).withColumn("minute",minuteUDF(col("sn-start-time")))
    dataWithHourMinute.write.partitionBy("hour","minute").orc(filePath)
  }

}
